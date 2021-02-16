package org.apache.openwhisk.core.loadBalancer

/** States of invoker set assigned to each function **/
class ActionInvokerSetState(minShrinkInterval: Long) {
    // set size related
    protected var _numInvokers: Int = 1     // number of invokers in the set
    protected var _version: Long = 0
    protected var _minShrinkInterval: Long = minShrinkInterval  // in milliseconds
    protected var _prevShrinkTime: Long = 0
    protected var _homeUpdated: Boolean = false
    
    def numInvokers: Int = _numInvokers
    // return (if_update, next_num_invokers, version)
    def setHomeUpdated() {
        _homeUpdated = true
    }

    def update(next_num: Int, is_shrink: Boolean,
            assigned_version: Long): (Boolean, Int, Long) = {
        var update: Boolean = false
        var next_invokers: Int = _numInvokers
        var version: Long = _version
        var cur_time: Long = System.currentTimeMillis()
        // ignore updates if version is smaller than current
        // update _numInvokers
        if(next_num > 0 && assigned_version >= _version) {
            if(_homeUpdated) {
                // when homeInvoker is updated, set always needs updated
                // record
                _prevShrinkTime = cur_time
                _numInvokers = next_num
                _version = cur_time
                _homeUpdated = false
                // return data
                update = true
                next_invokers = _numInvokers
                version = _version
            } else if(is_shrink && cur_time - _prevShrinkTime >= _minShrinkInterval && next_num < _numInvokers) {
                // record
                _numInvokers = next_num
                _version = cur_time
                _prevShrinkTime = cur_time
                // return data
                update = true
                next_invokers = _numInvokers
                version = _version
            } else if(!is_shrink && next_num > _numInvokers) {
                // record
                _numInvokers = next_num
                _version = cur_time
                // return data
                update = true
                next_invokers = _numInvokers
                version = _version
            }
        }
        (update, next_invokers, version)
    }
}