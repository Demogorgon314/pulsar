package org.apache.pulsar.broker.loadbalance.test.reporter;

import org.apache.pulsar.broker.loadbalance.test.data.BundleLoadData;
import org.apache.pulsar.broker.loadbalance.test.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.test.data.TimeAverageBrokerLoadData;

import java.util.Map;

public class BundleLoadDataReporter extends AbstractLoadDataReporter<Map<String, BundleLoadData>> {

    private final LoadDataStore<BundleLoadData> loadDataStore;

    public BundleLoadDataReporter(LoadDataStore<BundleLoadData> loadDataStore) {
        this.loadDataStore = loadDataStore;
    }

    @Override
    public Map<String, BundleLoadData> generateLoadData() {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws Exception {

    }
}
