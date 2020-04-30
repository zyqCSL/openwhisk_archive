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
 * @param redudantRatio the ratio of docker_cpu_limit/estimated_tail_usage
 */
class Distribution(
        numCores: Int,
        updateBatch: Int,
        cpuUtilPercentile: Double,
        cpuLimitPercentile: Double,
        cpuUtilWindow: Int) {
    protected val _numCores: Int = numCores
    protected val _step: Double = 0.1

    protected val _numBins = (1/_step * _numCores).toInt

    protected val _updateBatch = updateBatch
    protected val _cpuUtilPercentile = cpuUtilPercentile
    protected val _cpuLimitPercentile = cpuLimitPercentile
    protected val _cpuUtilWindow = cpuUtilWindow

    protected var samples: Array[Long] = Array.fill(_numBins)(0)
    protected var window: Array[Double] = Array.fill(_cpuUtilWindow)(0.0)

    protected var window_size: Int = 0
    protected var window_ptr: Int = 0

    protected var numNewSamples: Long = 0
    protected var numSamples: Long = 0

    def addSample(sample: Double, useExpectation: Boolean): (Double, Double) =  {
        window(window_ptr) = sample
        window_ptr  = if(window_ptr  == _cpuUtilWindow - 1) { 0 } else { window_ptr + 1 }
        window_size = if(window_size == _cpuUtilWindow)     { window_size } else { window_size + 1 }

        var cpu_limit: Double = 0.0
        var cpu_limit_window: Double = 0.0

        var estimated_cpu: Double = 0.0

        numSamples = numSamples + 1
        numNewSamples = numNewSamples + 1
        // update distribution
        var slot: Int = (math.ceil(sample/_step)).toInt
        if(slot >= _numBins)
            slot = _numBins - 1
        samples(slot) = samples(slot) + 1

        if(numNewSamples >= _updateBatch) {
            numNewSamples = 0
            
            var i: Int = 0
            if(useExpectation) {
                var accum_val: Long = 0
                while(i < _numBins) {
                    accum_val = accum_val + samples(i)*(i + 1)
                    i = i + 1
                }
                estimated_cpu = accum_val * _step/numSamples
            } else {
                var cpu_limit_unknown: Boolean = true
                var est_cpu_unknown: Boolean = true
                var accum_samples: Long = 0
                while(i < _numBins && ( cpu_limit_unknown || est_cpu_unknown )) {
                    accum_samples = accum_samples + samples(i)
                    var cur_percent: Double = accum_samples.toDouble/numSamples
                    if(cur_percent >= _cpuUtilPercentile && est_cpu_unknown) {
                        est_cpu_unknown = false
                        estimated_cpu = i*_step
                        if(i == 0)
                            estimated_cpu = _step
                    }

                    if(cur_percent >= _cpuLimitPercentile && cpu_limit_unknown) {
                        cpu_limit_unknown = false
                        cpu_limit = i*_step
                        if(i == 0)
                            cpu_limit = _step
                    }

                    i = i + 1
                }
            }
        }

        if(window_size == _cpuUtilWindow)
            cpu_limit_window = window.sum/_cpuUtilWindow
        else 
            cpu_limit_window = window.take(window_size).sum/window_size

        if(cpu_limit_window > cpu_limit)
            cpu_limit = cpu_limit_window

        (math.ceil(estimated_cpu*10).toInt/10.0, cpu_limit)
    }


    def showDistr() {
        var i = 0
        while(i < _numBins) {
            println(s"${math.ceil((i + 1)*_step * 10).toInt/10.0}:${samples(i)}")
            i = i + 1
        }
    }
}