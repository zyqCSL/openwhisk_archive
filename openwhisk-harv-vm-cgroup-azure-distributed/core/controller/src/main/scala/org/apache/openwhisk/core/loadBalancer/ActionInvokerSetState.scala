package org.apache.openwhisk.core.loadBalancer

/** States of invoker set assigned to each function **/
class ActionInvokerSetState(minShrinkInterval: Long) {
    protected var _minShrinkInterval: Long = minShrinkInterval  // in milliseconds
    protected var _numInvokers: Int = 1
    protected var _version: Long = 0
    protected var _prevShrinkTime: Long = 0
    
    def numInvokers: Int = _numInvokers
    // return (if_update, next_num_invokers, new version)
    def updateNumInvokers(next_num: Int, is_shrink: Boolean, assigned_version: Long): (Boolean, Int, Long) = {
        var update: Boolean = false
        var next_invokers: Int = _numInvokers
        var next_version: Long = _version
        var cur_time: Long = System.currentTimeMillis()
        // ignore updates if version is smaller than current
        if(assigned_version >= _version) {
            if(is_shrink && cur_time - _prevShrinkTime >= _minShrinkInterval && next_num < _numInvokers) {
                // record
                _numInvokers = next_num
                _version = cur_time
                _prevShrinkTime = cur_time
                // return data
                update = true
                next_invokers = next_num
                next_version = _version
            } else if(!is_shrink && next_num > _numInvokers) {
                // record
                _numInvokers = next_num
                _version = cur_time
                // return data
                update = true
                next_invokers = next_num
                next_version = _version
            }
        }
        (update, next_invokers, next_version)
    }
}