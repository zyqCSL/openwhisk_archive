package org.apache.openwhisk.core.loadBalancer

/** States of invoker set assigned to each function **/
class ActionInvokerSetState(minShrinkInterval: Long) {
    protected var _minShrinkInterval: Long = minShrinkInterval  // in milliseconds
    protected var _homeInvoker: Int = 0
    protected var _numInvokers: Int = 1     // number of invokers in the set
    protected var _numLiveInvokers: Int = 0 // number of live invokers when homeInvoker is decided
    protected var _version: Long = 0
    protected var _homeVersion: Long = 0    // the version number wrt previous homeInvoker update
    protected var _prevShrinkTime: Long = 0
    
    def numInvokers: Int = _numInvokers
    // return (if_update_home, if_update_size, next_home, next_num_invokers, home_version, size_version)
    // next_home = -1 then homeInvoker is not updated
    // next_num = -1 then numInvokers is not updated
    def updateNumInvokers(next_home: Int, next_num: Int, num_live: Int,
            is_shrink: Boolean, 
            assigned_version: Long): (Boolean, Boolean, Int, Long) = {
        var update_home: Boolean = false
        var update_size: Boolean = false
        var next_home_inv: Int = _homeInvoker
        var next_invokers: Int = _numInvokers
        var home_version: Long = _homeVersion
        var size_version: Long = _version
        var home_version: Long
        var cur_time: Long = System.currentTimeMillis()
        // ignore updates if version is smaller than current
        // update _homeInvoker
        if(next_home >= 0 && assigned_version >= _homeVersion) {
            _homeInvoker = next_home
            _homeVersion = cur_time
            _numLiveInvokers = num_live
            update_home = true
            home_version = _homeVersion
            next_home_inv = _homeInvoker
        }
        // update _numInvokers
        if(next_num > 0 && assigned_version >= _version) {
            if(is_shrink && cur_time - _prevShrinkTime >= _minShrinkInterval && next_num < _numInvokers) {
                // record
                _numInvokers = next_num
                _version = cur_time
                _prevShrinkTime = cur_time
                // return data
                update_size = true
                next_invokers = _numInvokers
                size_version = _version
            } else if(!is_shrink && next_num > _numInvokers) {
                // record
                _numInvokers = next_num
                _version = cur_time
                // return data
                update_size = true
                next_invokers = _numInvokers
                size_version = _version
            }
        }
        (update_home, update_size, next_home_inv, next_invokers, size_version)
    }
}