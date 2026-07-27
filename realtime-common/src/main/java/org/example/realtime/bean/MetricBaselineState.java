package org.example.realtime.bean;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** 维护最近一段时间的有界滚动基线，避免永久累计均值掩盖业务变化。 */
@Data
@NoArgsConstructor
public class MetricBaselineState implements Serializable {
    private long sampleCount;
    private double mean;
    private double m2;
    private List<Long> samples = new ArrayList<Long>();
    private long lastAnomalyTimestamp;

    public void add(long value, int maxSamples) {
        if (samples == null) {
            samples = new ArrayList<Long>();
        }
        samples.add(value);
        if (samples.size() > maxSamples) {
            samples.remove(0);
        }
        recalculate();
    }

    private void recalculate() {
        sampleCount = samples.size();
        mean = 0D;
        m2 = 0D;
        long count = 0L;
        for (Long sample : samples) {
            count++;
            double delta = sample - mean;
            mean += delta / count;
            double delta2 = sample - mean;
            m2 += delta * delta2;
        }
    }

    public double getStandardDeviation() {
        if (sampleCount < 2) {
            return 0D;
        }
        return Math.sqrt(m2 / (sampleCount - 1));
    }
}
