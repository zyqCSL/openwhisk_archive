package org.apache.openwhisk.core.loadBalancer

/** States of invoker set assigned to each function **/
class ActionInvokerSetState(minShrinkInterval: Long) {
    // homeInvoker related
    protected var _homeInvoker: Int = 0
    protected var _numLiveInvokers: Int = 0 // number of live invokers when homeInvoker is decided
    protected var _homeVersion: Long = 0    // the version number wrt previous homeInvoker update
    
    // set size related
    protected var _numInvokers: Int = 1     // number of invokers in the set
    protected var _sizeVersion: Long = 0
    protected var _minShrinkInterval: Long = minShrinkInterval  // in milliseconds
    protected var _prevShrinkTime: Long = 0
    
    def homeInvoker: Int = _homeInvoker
    def numLiveInvokers: Int = _numLiveInvokers
    def numInvokers: Int = _numInvokers

    // return (if_update_home, next_home, num_live_invokers, home_version)
    def updateHomeInvoker(next_home: Int, num_live: Int, 
            assigned_version: Long): (Boolean, Int, Int, Long) = {
        var update_home: Boolean = false
        var next_home_inv: Int = _homeInvoker
        var num_live_inv: Int = _numLiveInvokers
        var home_version: Long = _homeVersion
        var cur_time: Long = System.currentTimeMillis()
        // ignore updates if version is smaller than current
        // update _homeInvoker
        if(next_home >= 0 && assigned_version >= _homeVersion) {
            // record
            _homeInvoker = next_home
            _homeVersion = cur_time
            _numLiveInvokers = num_live
            // return data
            update_home = true
            home_version = _homeVersion
            next_home_inv = _homeInvoker
            num_live_inv = _numLiveInvokers
        }
        (update_home, next_home_inv, num_live_inv, home_version)
    }

    // return (if_update_size, next_num_invokers, size_version)
    def updateNumInvokers(next_num: Int, 
            update_home: Boolean, is_shrink: Boolean,
            assigned_version: Long): (Boolean, Int, Long) = {
        var update_size: Boolean = false
        var next_invokers: Int = _numInvokers
        var size_version: Long = _sizeVersion
        var cur_time: Long = System.currentTimeMillis()
        // ignore updates if version is smaller than current
        // update _numInvokers
        if(next_num > 0 && assigned_version >= _sizeVersion) {
            if(update_home) {
                // when homeInvoker is updated, set always needs updated
                // record
                _prevShrinkTime = cur_time
                _numInvokers = next_num
                _sizeVersion = cur_time
                // return data
                update_size = true
                next_invokers = _numInvokers
                size_version = _sizeVersion
            } else if(is_shrink && cur_time - _prevShrinkTime >= _minShrinkInterval && next_num < _numInvokers) {
                // record
                _numInvokers = next_num
                _sizeVersion = cur_time
                _prevShrinkTime = cur_time
                // return data
                update_size = true
                next_invokers = _numInvokers
                size_version = _sizeVersion
            } else if(!is_shrink && next_num > _numInvokers) {
                // record
                _numInvokers = next_num
                _sizeVersion = cur_time
                // return data
                update_size = true
                next_invokers = _numInvokers
                size_version = _sizeVersion
            }
        }
        (update_size, next_invokers, size_version)
    }
}