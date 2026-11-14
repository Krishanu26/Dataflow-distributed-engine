package com.dataflow.job;

import org.springframework.stereotype.Component;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class JobStore {
    private final ConcurrentHashMap<String, ProcessingJob> jobs = new ConcurrentHashMap<>();
    public void save(ProcessingJob job) { jobs.put(job.getId(), job); }
    public Optional<ProcessingJob> find(String id) { return Optional.ofNullable(jobs.get(id)); }
    public Collection<ProcessingJob> findAll() {
        return jobs.values().stream().sorted(Comparator.comparing(ProcessingJob::getSubmittedAt).reversed()).toList();
    }
}
