import traceback

from celery.worker.control import Panel
from celery.registry import tasks as tasks_registry
from celery.task.base import BaseTask

#==============================================================================#
def _get_task_class(name):
    try:
        task = tasks_registry[name]
    except tasks_registry.NotRegistered:
        print 'policy.control: ERROR: task {0} was not found'.format(name)
        raise
    assert isinstance(task, BaseTask), 'type(task): {0}'.format(type(task))
    return task.__class__


@Panel.register
def update_tasks_settings(panel, tasks_settings):
    # task_settings = dict: { taskname: { attrname: value, ... }, ... }
    panel.logger.debug('policy.control: update_tasks_settings()')
    errors = []
    for taskname, settings in tasks_settings.iteritems():
        try:
            taskcls = _get_task_class(taskname)
            for attr,val in settings.iteritems():
                setattr(taskcls, attr, val)
        except Exception as e:
            errors.append((taskname, settings, traceback.format_ex()))
    if errors:
        msg = 'Errors occurred while attempting to update task settings.\n'
        for taskname, settings, tb in errors:
            m = 'Taskname: {0}\nSettings: {1}\n\n'.format(taskname, settings)
            m += '{0}\n'.format(tb)
            msg += m
        panel.logger.error(msg)
        
        errmsg = "Exceptions occurred while updating task settings.  "
        errmsg += "See the celeryd log for details."
        return {"error": errmsg}
    else:
        return {"ok": "task settings updated"}
    

@Panel.register
def get_task_settings(panel, tasknames=None, setting_names=None):
    """ Returns the settings actually set on a Task class without taking any 
        inheritance into account.  It does *not* get settings from superclasses. 
    """
    panel.logger.debug('policy.control: get_task_settings()')
    if tasknames is None:
        tasknames = tasks_registry.data
    allsettings = {}
    errors = []
    for taskname in tasknames:
        try:
            taskcls = _get_task_class(taskname)
            data = dict((attr, taskcls.__dict__[attr]) for attr in setting_names 
                                                       if attr in taskcls.__dict__)
            allsettings[taskname] = data
        except Exception as e:
            errors.append((taskname, traceback.format_ex()))
    if errors:
        msg = 'Errors occurred while attempting to retrieve task settings.\n'
        for taskname, tb in errors:
            m = 'Taskname: {0}\n\n{1}\n'.format(taskname,tb)
            msg += m
        panel.logger.error(msg)
        
        errmsg = "Exceptions occurred while retrieving task settings.  "
        errmsg += "See the celeryd log for details."
        return {"error": errmsg}
        
    return allsettings
    
    
@Panel.register
def get_all_task_settings(panel, setting_names):
    """ Returns the settings actually set on a Task class without taking any 
        inheritance into account.  It does *not* get settings from superclasses. 
    """
    panel.logger.debug('policy.control: get_all_task_settings()')
    allsettings = {}
    for taskname in tasks_registry.data:
        taskcls = _get_task_class(taskname)    
        data = dict((attr, taskcls.__dict__[attr]) for attr in setting_names 
                                                   if attr in taskcls.__dict__)
        allsettings[taskname] = data
    return allsettings


@Panel.register
def restore_task_settings(panel, restore_data):
    # restore_data = dict: { taskname: (restore_dict, erase_list), ... }
    panel.logger.debug('policy.control: restore_task_settings()\nrestore_data: {0}'.format(restore_data))
    
    errors = []
    for taskname, (restore, erase) in restore_data.iteritems():
        try:
            taskcls = _get_task_class(taskname)
        except tasks_registry.NotRegistered as e:
            errors.append((taskname, traceback.format_ex()))
            continue
        
        for attr,v in restore.iteritems():
            panel.logger.debug('restoring: {0}.{1} = {2}'.format(taskname, attr, v))
            setattr(taskcls, attr, v)
        for attr in erase:
            panel.logger.debug('erasing: {0}.{1}'.format(taskname, attr))
            try:
                delattr(taskcls, attr)
            except AttributeError:
                panel.logger.warning('restore_task_settings(): could not delete {0}.{1}'.format(taskname, attr))
                pass
    if errors:
        msg = 'Errors occurred while attempting to restore task settings.\n'
        for taskname, tb in errors:
            m = 'Taskname: {0}\n\n{1}\n'.format(taskname,tb)
            msg += m
        panel.logger.error(msg)
        
        errmsg = "Exceptions occurred while restoring task settings.  "
        errmsg += "See the celeryd log for details."
        return {"error": errmsg}
    else:
        return {"ok": "task settings restored"}
        

#==============================================================================#
@Panel.register
def get_task_attribute(panel, taskname, attrname):
    panel.logger.debug('policy.control: get_task_attribute()')
    try:
        taskcls = _get_task_class(taskname)
    except KeyError:
        panel.logger.error("Attempted to get %s for unknown task %s" % (
            attrname, taskname), exc_info=sys.exc_info())
        return {"error": "unknown task"}
    
    if not hasattr(taskcls, attrname):
        msg = "Attempted to get an unknown attribute %s for task %s" % (
               attrname, taskname)
        panel.logger.error(msg)
        return {"error": "unknown attribute"}
        
    return getattr(taskcls, attrname)


@Panel.register
def set_task_attribute(panel, tasknames, attrname, value):
    panel.logger.debug('policy.control: set_task_attribute()')
    if isinstance(tasknames, basestring):
        tasknames = [tasknames]
    for taskname in tasknames:
        try:
            taskcls = _get_task_class(taskname)
        except KeyError:
            panel.logger.error("Attempted to set %s for unknown task %s" % (
                attrname, taskname), exc_info=sys.exc_info())
            return {"error": "unknown task"}
        
        if not hasattr(taskcls, attrname):
            msg = "Attempted to set an unknown attribute %s for task %s" % (
                   attrname, taskname)
            panel.logger.error(msg)
            return {"error": "unknown attribute"}
        
        setattr(taskcls, attrname, value)
    return {"ok": "new %s set successfully" % attrname}

#==============================================================================#
@Panel.register
def prefetch_increment(panel, n):
    try:
        panel.consumer.qos.increment(n)
    except Exception as e:
        m = 'Error occurred while attempting to increment the worker prefetch:'
        m += '\n\n' + traceback.format_ex()
        panel.logger.error(m)
        
        errmsg = "An exception occurred while incrementing the worker prefetch."
        errmsg += "  See the celeryd log for details."
        return {"error": errmsg}
    return {'ok': 'incremented prefetch by {0}'.format(n) }

@Panel.register
def prefetch_decrement(panel, n):
    try:
        panel.consumer.qos.decrement(n)
    except Exception as e:
        m = 'Error occurred while attempting to decrement the worker prefetch:'
        m += '\n\n' + traceback.format_ex()
        panel.logger.error(m)
        
        errmsg = "An exception occurred while decrementing the worker prefetch."
        errmsg += "  See the celeryd log for details."
        return {"error": errmsg}
    return {'ok': 'decremented prefetch by {0}'.format(n) }

#==============================================================================#

