//! The `async-jobs` crate provides a framework for describing and executing a collection
//! of interdependent and asynchronous jobs. It is intended to be used as the scheduling
//! backbone for programs such as build systems which need to orchestrate arbitrary
//! collections of tasks with complex dependency graphs.
//!
//! The main way to use this crate is by providing an implementation of the `Job`
//! trait to describe the tasks in your program and how they depend on one another.
//! To run your tasks, pass an instance of your `Job` to the `Scheduler::run` method.

use async_trait::async_trait;

// todo: docs
#[async_trait]
pub trait Job: Sized + PartialEq {

    /// Error type returned by the implementation
    type Error;

    /// Returns the list of jobs that this job depends on
    fn dependencies(&self) -> Vec<Self>;

    /// Performs the work associated with this job
    async fn run(&self) -> Result<(), Self::Error>;
}

/// Data structure used to schedule execution of dependent jobs
struct Schedule<J> {

    /// List of jobs which comprise this schedule
    jobs: Vec<J>,

    /// List of indices (into `self.jobs`) in order of execution
    queue: Vec<usize>,
}

impl<J: Job> Schedule<J> {

    /// Creates a new schedule for executing `job` and its dependencies
    fn new(job: J) -> Self {

        let mut sched = Self {
            jobs: Default::default(),
            queue: Default::default(),
        };

        sched.add_job(job);

        sched
    }

    /// Adds `job` and its dependencies to this schedule
    fn add_job(&mut self, job: J) -> usize {

        let job_idx = self.jobs.len();
        self.jobs.push(job);

        for dep in self.jobs[job_idx].dependencies() {
            if !self.jobs.iter().any(|j| *j == dep) {
                self.add_job(dep);
            }
        }

        self.queue.push(job_idx);
        job_idx
    }
}

/// Schedules execution of jobs and dependencies
///
/// Uses the builder pattern to configure various aspects of job execution.
pub struct Scheduler(());

impl Scheduler {

    /// Creates a new scheduler with default settings
    pub fn new() -> Self {
        Scheduler(())
    }

    /// Executes `job` and its dependencies on this scheduler
    pub async fn run<J: Job>(&self, job: J) -> Result<(), J::Error> {

        let sched = Schedule::new(job);

        for i in sched.queue {
            sched.jobs[i].run().await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use async_std::sync::Mutex;

    use super::*;

    type TestGraph = Vec<(bool, Vec<usize>)>;

    struct TestJob<'a> {
        index: usize,
        graph: &'a TestGraph,
        trace: &'a Mutex<Vec<usize>>,
        success: bool,
    }

    #[async_trait]
    impl<'a> Job for TestJob<'a> {

        type Error = usize;

        fn dependencies(&self) -> Vec<Self> {

            let mut deps = vec![];

            for index in &self.graph[self.index].1 {
                deps.push(TestJob {
                    index: *index,
                    graph: self.graph,
                    trace: self.trace,
                    success: self.graph[*index].0,
                })
            }

            deps
        }

        async fn run(&self) -> Result<(), Self::Error> {

            self.trace.lock().await
                .push(self.index);

            if self.success {
                Ok(())
            } else {
                Err(self.index)
            }
        }
    }

    impl<'a> PartialEq for TestJob<'a> {
        fn eq(&self, other: &Self) -> bool {
            self.index == other.index
        }
    }

    async fn trace(graph: TestGraph) -> (Vec<Option<usize>>, Option<usize>) {

        let trace = Mutex::new(vec![]);
        let job = TestJob {
            index: 0,
            graph: &graph,
            trace: &trace,
            success: graph[0].0,
        };

        let sched = Scheduler::new();
        let failed_idx = sched.run(job)
            .await
            .err();

        let mut results = vec![None; graph.len()];

        for (finished_idx, job_idx) in trace.into_inner().iter().enumerate() {

            // Ensure no job has had its update method called more than once
            assert!(results[*job_idx].is_none());

            results[*job_idx] = Some(finished_idx);
        }

        (results, failed_idx)
    }

    #[async_std::test]
    async fn single_job() {

        let (trace, failed) = trace(vec![
            (true, vec![]),
        ]).await;

        assert!(failed.is_none());
        assert_eq!(trace[0], Some(0));
    }

    #[async_std::test]
    async fn single_job_fails() {

        let (trace, failed) = trace(vec![
            (false, vec![]),
        ]).await;

        assert_eq!(failed, Some(0));
        assert_eq!(trace[0], Some(0));
    }

    #[async_std::test]
    async fn single_dep() {

        let (trace, failed) = trace(vec![
            (true, vec![1]),
            (true, vec![]),
        ]).await;

        assert!(failed.is_none());
        assert_eq!(trace[0], Some(1));
        assert_eq!(trace[1], Some(0));
    }

    #[async_std::test]
    async fn single_dep_fails() {

        let (trace, failed) = trace(vec![
            (true, vec![1]),
            (false, vec![]),
        ]).await;

        assert_eq!(failed, Some(1));
        assert_eq!(trace[0], None);
        assert_eq!(trace[1], Some(0));
    }

    #[async_std::test]
    async fn single_dep_root_fails() {

        let (trace, failed) = trace(vec![
            (false, vec![1]),
            (true, vec![]),
        ]).await;

        assert_eq!(failed, Some(0));
        assert_eq!(trace[0], Some(1));
        assert_eq!(trace[1], Some(0));
    }

    #[async_std::test]
    async fn two_deps() {

        let (trace, failed) = trace(vec![
            (true, vec![1, 2]),
            (true, vec![]),
            (true, vec![]),
        ]).await;

        assert!(failed.is_none());
        assert_eq!(trace[0], Some(2));
        assert!(matches!(trace[1], Some(x) if x < 2));
        assert!(matches!(trace[2], Some(x) if x < 2));
    }

    #[async_std::test]
    async fn two_deps_one_fails() {

        let (trace, failed) = trace(vec![
            (true, vec![1, 2]),
            (true, vec![]),
            (false, vec![]),
        ]).await;

        assert_eq!(failed, Some(2));
        assert_eq!(trace[0], None);
        // job 1 may or may not be updated
        assert!(trace[2].is_some());
    }

    #[async_std::test]
    async fn single_trans_dep() {

        let (trace, failed) = trace(vec![
            (true, vec![1]),
            (true, vec![2]),
            (true, vec![]),
        ]).await;

        assert!(failed.is_none());
        assert_eq!(trace[0], Some(2));
        assert_eq!(trace[1], Some(1));
        assert_eq!(trace[2], Some(0));
    }

    #[async_std::test]
    async fn single_trans_dep_fails() {

        let (trace, failed) = trace(vec![
            (true, vec![1]),
            (true, vec![2]),
            (false, vec![]),
        ]).await;

        assert_eq!(failed, Some(2));
        assert_eq!(trace[0], None);
        assert_eq!(trace[1], None);
        assert_eq!(trace[2], Some(0));
    }

    #[async_std::test]
    async fn single_trans_dep_direct_dep_fails() {

        let (trace, failed) = trace(vec![
            (true, vec![1]),
            (false, vec![2]),
            (true, vec![]),
        ]).await;

        assert_eq!(failed, Some(1));
        assert_eq!(trace[0], None);
        assert_eq!(trace[1], Some(1));
        assert_eq!(trace[2], Some(0));
    }

    #[async_std::test]
    async fn two_deps_single_trans_dep() {

        let (trace, failed) = trace(vec![
            (true, vec![1, 3]),
            (true, vec![2]),
            (true, vec![]),
            (true, vec![]),
        ]).await;

        assert!(failed.is_none());
        assert_eq!(trace[0], Some(3));
        assert!(matches!(trace[3], Some(x) if x < 3));

        let order_of_1 = trace[1].unwrap();
        let order_of_2 = trace[2].unwrap();
        assert!(order_of_1 > order_of_2);
        assert!(order_of_1 < 3);
    }

    #[async_std::test]
    async fn two_deps_each_with_trans_dep() {

        let (trace, failed) = trace(vec![
            (true, vec![1, 3]),
            (true, vec![2]),
            (true, vec![]),
            (true, vec![4]),
            (true, vec![]),
        ]).await;

        assert!(failed.is_none());
        assert_eq!(trace[0], Some(4));

        let order_of_1 = trace[1].unwrap();
        let order_of_2 = trace[2].unwrap();
        assert!(order_of_1 < 4);
        assert!(order_of_2 < 4);
        assert!(order_of_1 > order_of_2);

        let order_of_3 = trace[3].unwrap();
        let order_of_4 = trace[4].unwrap();
        assert!(order_of_3 < 4);
        assert!(order_of_4 < 4);
        assert!(order_of_3 > order_of_4);
    }

    #[async_std::test]
    async fn three_deps() {

        let (trace, failed) = trace(vec![
            (true, vec![1, 2, 3]),
            (true, vec![]),
            (true, vec![]),
            (true, vec![]),
        ]).await;

        assert!(failed.is_none());
        assert_eq!(trace[0], Some(3));
        assert!(matches!(trace[1], Some(x) if x < 3));
        assert!(matches!(trace[2], Some(x) if x < 3));
        assert!(matches!(trace[3], Some(x) if x < 3));
    }

    #[async_std::test]
    async fn diamond() {

        let (trace, failed) = trace(vec![
            (true, vec![2, 3]),
            (true, vec![]),
            (true, vec![1]),
            (true, vec![1]),
        ]).await;

        assert!(failed.is_none());
        assert_eq!(trace[0], Some(3));
        assert_eq!(trace[1], Some(0));

        let order_of_2 = trace[2].unwrap();
        let order_of_3 = trace[3].unwrap();
        assert!(order_of_2 > 0);
        assert!(order_of_2 < 3);
        assert!(order_of_3 > 0);
        assert!(order_of_3 < 3);
    }

    #[async_std::test]
    async fn diamond_with_extra_trans_deps() {

        let (trace, failed) = trace(vec![
            (true, vec![2, 3]),
            (true, vec![4]),
            (true, vec![1, 5]),
            (true, vec![1, 6]),
            (true, vec![]),
            (true, vec![]),
            (true, vec![]),
        ]).await;

        assert!(failed.is_none());
        assert_eq!(trace[0], Some(6));

        let order_of_2 = trace[2].unwrap();
        assert!(order_of_2 < 6);

        let order_of_3 = trace[3].unwrap();
        assert!(order_of_3 < 6);

        let order_of_1 = trace[1].unwrap();
        assert!(order_of_1 < order_of_2);
        assert!(order_of_1 < order_of_3);

        let order_of_4 = trace[4].unwrap();
        assert!(order_of_4 < order_of_1);

        let order_of_5 = trace[5].unwrap();
        assert!(order_of_5 < order_of_2);

        let order_of_6 = trace[6].unwrap();
        assert!(order_of_6 < order_of_3);
    }
}
