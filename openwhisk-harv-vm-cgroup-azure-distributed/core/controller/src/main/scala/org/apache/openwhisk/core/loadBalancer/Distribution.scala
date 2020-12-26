package org.apache.openwhisk.core.loadBalancer

import scala.math

/**
 * Distribution (for function cpu utilization)
 *
 * @param numBins total samples (latest) that the distribution holds
 * @param updateBatch distribution is updated in batch, when #updateBatch samples are available
 * @param cpuUtilPercentile the percentile for load balancing estimation
 * @param cpuLimitPercentile the percentile to set docker --cpus
 * @param cpuUtilWindow window to calculate cpu limit
 * @param exeTimePercentile the percentile for estimating execution time
 * @param redudantRatio the ratio of docker_cpu_limit/estimated_tail_usage
 */
class Distribution(
        numCores: Int,
        maxTime: Long,
        updateBatch: Int,
        cpuUtilPercentile: Double,
        cpuLimitPercentile: Double,
        exeTimePercentile: Double
        // cpuUtilWindow: Int
        ) {
    protected val _numCores: Int = numCores
    protected val _maxTime: Long = maxTime
    protected val _cpuStep: Double = 0.1
    protected val _timeStep: Long = 100 // unit is ms

    protected val _numCpuBins = (1/_cpuStep * _numCores).toInt
    protected val _numTimeBins = (maxTime/_timeStep).toInt

    protected val _updateBatch = updateBatch
    protected val _cpuUtilPercentile = cpuUtilPercentile
    protected val _cpuLimitPercentile = cpuLimitPercentile
    // protected val _cpuUtilWindow = cpuUtilWindow
    protected val _exeTimePercentile = exeTimePercentile
    
    // histograms
    protected var cpuSamples: Array[Long] = Array.fill(_numCpuBins)(0)
    protected var timeSamples: Array[Long] = Array.fill(_numTimeBins)(0)
    // protected var window: Array[Double] = Array.fill(_cpuUtilWindow)(0.0)

    // protected var window_size: Int = 0
    // protected var window_ptr: Int = 0

    protected var numNewSamples: Long = 0
    protected var numSamples: Long = 0

    protected var accum_cpu_time_product: Long = 0
    protected var accum_exe_time: Long = 0
    protected var maxRecordedTime: Long = 0
    
    // exeTime unit is ms
    // return (cpu_util, cpu_limit, exe_time)
    def addSample(cpu: Double, exeTime: Long, useExpectation: Boolean): (Double, Double, Long) =  {
        // use exeTime > 0 to filter timeout handler
        if(exeTime > 0) {
            // window(window_ptr) = cpu
            // window_ptr  = if(window_ptr  == _cpuUtilWindow - 1) { 0 } else { window_ptr + 1 }
            // window_size = if(window_size == _cpuUtilWindow)     { window_size } else { window_size + 1 }
            accum_exe_time = accum_exe_time + exeTime
            accum_cpu_time_product = accum_cpu_time_product + (cpu*exeTime).toLong
            maxRecordedTime = math.max(exeTime, maxRecordedTime)

            // update distribution
            numSamples = numSamples + 1
            numNewSamples = numNewSamples + 1
            var cpu_slot: Int = (math.ceil(cpu/_cpuStep)).toInt
            var time_slot: Int = (math.ceil(exeTime/_timeStep)).toInt
            if(cpu_slot >= _numCpuBins)
                cpu_slot = _numCpuBins - 1
            if(time_slot >= _numTimeBins)
                time_slot = _numTimeBins - 1
            cpuSamples(cpu_slot) = cpuSamples(cpu_slot) + 1
            timeSamples(time_slot) = timeSamples(time_slot) + 1
        }

        var cpu_limit: Double = 0.0
        // var cpu_limit_window: Double = 0.0

        var estimated_cpu: Double = 0
        var estimated_time: Long = 0  // unit is ms

        if(numNewSamples >= _updateBatch) {
            numNewSamples = 0
            
            var i: Int = 0
            if(useExpectation) {
                // var accum_val: Long = 0
                // while(i < _numCpuBins) {
                //     accum_val = accum_val + cpuSamples(i)*(i + 1)
                //     i = i + 1
                // }
                // estimated_cpu = accum_val * _cpuStep/numSamples
                estimated_cpu = accum_cpu_time_product.toDouble / accum_exe_time
                estimated_time = math.max((accum_exe_time / numSamples).toLong, 1)
                // always compute cpu limit based on percentile
                var cpu_limit_unknown: Boolean = true
                var accum_samples: Long = 0
                while(i < _numCpuBins && cpu_limit_unknown) {
                    accum_samples = accum_samples + cpuSamples(i)
                    var cur_percent: Double = accum_samples.toDouble/numSamples
                    if(cur_percent >= _cpuLimitPercentile && cpu_limit_unknown) {
                        cpu_limit_unknown = false
                        cpu_limit = i*_cpuStep
                        if(i == 0)
                            cpu_limit = _cpuStep
                    }
                    i = i + 1
                }
            } else {
                var cpu_limit_unknown: Boolean = true
                var est_cpu_unknown: Boolean = true
                var accum_samples: Long = 0
                // estimate cpu
                while(i < _numCpuBins && ( cpu_limit_unknown || est_cpu_unknown )) {
                    accum_samples = accum_samples + cpuSamples(i)
                    var cur_percent: Double = accum_samples.toDouble/numSamples
                    if(cur_percent >= _cpuUtilPercentile && est_cpu_unknown) {
                        est_cpu_unknown = false
                        estimated_cpu = i*_cpuStep
                        if(i == 0)
                            estimated_cpu = _cpuStep
                    }

                    if(cur_percent >= _cpuLimitPercentile && cpu_limit_unknown) {
                        cpu_limit_unknown = false
                        cpu_limit = i*_cpuStep
                        if(i == 0)
                            cpu_limit = _cpuStep
                    }

                    i = i + 1
                }

                // estimate exe time
                var j: Int = 0
                var max_index: Int = math.min(_numTimeBins, 
                    (math.ceil(maxRecordedTime/_timeStep)).toInt + 1)
                var time_unknown: Boolean = true
                accum_samples = 0
                while(j < max_index && time_unknown) {
                    accum_samples = accum_samples + timeSamples(j)
                    var cur_percent: Double = accum_samples.toDouble/numSamples
                    if(cur_percent >= _exeTimePercentile && time_unknown) {
                        time_unknown = false
                        estimated_time = j*_timeStep
                        if(j == 0)
                            estimated_time = _timeStep
                    }
                    j = j + 1
                }

            }
        }

        // if(window_size == _cpuUtilWindow)
        //     cpu_limit_window = window.sum/_cpuUtilWindow
        // else 
        //     cpu_limit_window = window.take(window_size).sum/window_size

        // if(cpu_limit_window > cpu_limit)
        //     cpu_limit = cpu_limit_window

        (math.ceil(estimated_cpu*10).toInt/10.0, cpu_limit, estimated_time)
    }


    def showCpuDistr() {
        var i = 0
        while(i < _numCpuBins) {
            println(s"${math.ceil((i + 1)*_cpuStep * 10).toInt/10.0} cpus:${cpuSamples(i)}")
            i = i + 1
        }
    }

    def showTimeDistr() {
        var i = 0
        while(i < _numTimeBins) {
            println(s"${(i + 1)*_timeStep} ms:${timeSamples(i)}")
            i = i + 1
        }
    }
}