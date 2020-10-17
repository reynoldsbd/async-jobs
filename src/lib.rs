//! The `async-jobs` crate provides a framework for describing and executing a collection
//! of interdependent and asynchronous jobs. It is intended to be used as the scheduling
//! backbone for programs such as build systems which need to orchestrate arbitrary
//! collections of tasks with complex dependency graphs.
//!
//! The main way to use this crate is by providing an implementation of the `Job`
//! trait to describe the tasks in your program and how they depend on one another.
//! To run your tasks, pass an instance of your `Job` to the `Scheduler::run` method.

use std::collections::HashSet;
use std::mem;

use async_trait::async_trait;

#[non_exhaustive]
pub enum Outcome {
    Success,
}

/// A unit of work, perhaps with dependencies
///
/// Use the `Job` trait to describe the different types of jobs in your program
/// and how they depend on one another. An implementation has two responsibilities:
///
/// 1. Do some job-specific work when the `run` method is called
/// 2. Provide a list of dependency jobs via `dependencies`
///
/// Note that the return type of `dependencies` is `Vec<Self>`. This restriction
/// means that you cannot use different implementations of `Job` to represent different
/// types of work in your program; you must provide exactly one type. You may find
/// it useful to use an enum:
///
/// ```
/// use async_jobs::{Job, Outcome};
/// use async_trait::async_trait;
///
/// #[derive(PartialEq)]
/// enum MyJob {
///     DownloadFile(String),
///     ConcatFiles(Vec<String>),
/// }
///
/// #[async_trait]
/// impl Job for MyJob {
///
///     type Error = ();
///
///     async fn run(&self) -> Result<Outcome, Self::Error> {
///
///         match self {
///
///             MyJob::DownloadFile(file) => {
///                 // download file...
///             },
///
///             MyJob::ConcatFiles(files) => {
///                 // concatenate files...
///             },
///         }
///
///         Ok(Outcome::Success)
///     }
///
///     fn dependencies(&self) -> Vec<Self> {
///
///         match self {
///
///             MyJob::DownloadFile(_) => vec![],
///
///             MyJob::ConcatFiles(files) => {
///                 files.iter()
///                     .map(|f| MyJob::DownloadFile(f.clone()))
///                     .collect()
///             },
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait Job: Sized + PartialEq {

    /// Error type returned by the implementation
    type Error;

    /// Returns the list of jobs that this job depends on
    fn dependencies(&self) -> Vec<Self>;

    /// Performs the work associated with this job
    async fn run(&self) -> Result<Outcome, Self::Error>;
}

/// Errors returned by job scheduler
#[derive(Debug, PartialEq, Eq)]
pub enum Error<E> {

    /// Dependency cycle
    Cycle,

    /// One or more jobs failed
    Fail(Vec<E>),
}

/// Tracks the state of a scheduled job
enum JobState<J: Job> {

    /// Job has not yet started running
    Pending(J),

    /// Job is actively executing
    Running,

    /// Job failed execution
    Failed(J::Error),

    /// Job finished execution successfully
    Succeeded(Outcome),
}

impl<J: Job> JobState<J> {

    /// Returns whether or not this state equals `JobState::Succeeded`
    fn succeeded(&self) -> bool {
        match self {
            JobState::Succeeded(_) => true,
            _                      => false,
        }
    }
}

/// Bookkeeping information about a scheduled job
struct ScheduleEntry<J: Job> {

    /// Current state of corresponding job
    state: JobState<J>,

    /// List of jobs upon which this job depends
    dependencies: Vec<usize>,

    /// List of jobs which depend on this job
    dependents: Vec<usize>,
}

impl<J: Job> ScheduleEntry<J> {

    /// Returns a reference to the job held by this entry
    ///
    /// # Panics
    ///
    /// Panics if this is any other state besides `Pending`
    fn job(&self) -> &J {
        if let JobState::Pending(ref j) = self.state {
            j
        } else {
            panic!("unexpected job state")
        }
    }
}

impl<J: Job> From<J> for ScheduleEntry<J> {
    fn from(job: J) -> Self {
        Self {
            state: JobState::Pending(job),
            dependencies: Default::default(),
            dependents: Default::default(),
        }
    }
}

/// Data structure used to schedule execution of dependent jobs
struct Schedule<J: Job> {

    /// List of jobs which comprise this schedule
    jobs: Vec<ScheduleEntry<J>>,

    /// List of jobs that are ready to execute
    ready: Vec<usize>,
}

impl<J: Job> Schedule<J> {

    /// Creates a new schedule for executing `job` and its dependencies
    fn new(job: J) -> Result<Self, Error<J::Error>> {

        let mut sched = Self {
            jobs: Default::default(),
            ready: Default::default(),
        };

        let mut ancestors = Default::default();

        sched.add_job(job, &mut ancestors)?;

        Ok(sched)
    }

    /// Adds `job` and its dependencies to this schedule
    ///
    /// `ancestors` is used to detect and reject dependency cycles.
    fn add_job(&mut self, job: J, ancestors: &mut HashSet<usize>) -> Result<usize, Error<J::Error>> {

        let job_idx = self.jobs.len();
        self.jobs.push(job.into());

        assert!(ancestors.insert(job_idx));

        for dep in self.jobs[job_idx].job().dependencies() {

            let dep_idx = self.jobs.iter().position(|j| j.job() == &dep)
                .map(|i| if ancestors.contains(&i) { Err(Error::Cycle) } else { Ok(i) })
                .unwrap_or_else(|| self.add_job(dep, ancestors))?;

            self.jobs[dep_idx].dependents.push(job_idx);
            self.jobs[job_idx].dependencies.push(dep_idx);
        }

        assert!(ancestors.remove(&job_idx));

        if self.jobs[job_idx].dependencies.len() == 0 {
            self.ready.push(job_idx);
        }

        Ok(job_idx)
    }

    /// Returns the next job from the ready queue, along with its index
    fn next_job(&mut self) -> Option<(J, usize)> {

        if self.ready.len() == 0 {
            return None;
        }

        let idx = self.ready.remove(0);
        let state = mem::replace(&mut self.jobs[idx].state, JobState::Running);

        if let JobState::Pending(job) = state {
            Some((job, idx))
        } else {
            panic!("unexpected job status")
        }
    }

    /// Marks a job as completed and updates the ready queue with any new jobs that
    /// are now ready to execute as a result.
    fn mark_complete(&mut self, job_idx: usize, res: Result<Outcome, J::Error>) {

        self.jobs[job_idx].state = match res {
            Ok(outcome) => JobState::Succeeded(outcome),
            Err(err)    => JobState::Failed(err),
        };

        for dep_idx in &self.jobs[job_idx].dependents {
            let is_ready = self.jobs[*dep_idx].dependencies.iter()
                .all(|i| self.jobs[*i].state.succeeded());
            if is_ready {
                self.ready.push(*dep_idx);
            }
        }
    }
}

/// Schedules execution of jobs and dependencies
///
/// Uses the builder pattern to configure various aspects of job execution.
///
/// ```
/// use async_jobs::{Job, Outcome, Scheduler};
/// use async_trait::async_trait;
///
/// #[derive(PartialEq)]
/// struct MyJob(String);
///
/// #[async_trait]
/// impl Job for MyJob {
///
///     type Error = ();
///
///     async fn run(&self) -> Result<Outcome, Self::Error> {
///         println!("message: {}", self.0);
///         Ok(Outcome::Success)
///     }
///
///     fn dependencies(&self) -> Vec<Self> { vec![] }
/// }
///
/// Scheduler::default()
///     .run(MyJob("hello, world".into()));
/// ```
#[derive(Default)]
pub struct Scheduler(());

impl Scheduler {

    /// Executes `job` and its dependencies
    pub async fn run<J: Job>(&self, job: J) -> Result<(), Error<J::Error>> {

        let mut sched = Schedule::new(job)?;

        while let Some((job, idx)) = sched.next_job() {
            sched.mark_complete(idx, job.run().await);
        }

        let mut errs = vec![];
        for job in sched.jobs {
            if let JobState::Failed(err) = job.state {
                errs.push(err);
            }
        }

        if errs.len() > 0 {
            Err(Error::Fail(errs))
        } else {
            Ok(())
        }
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

        async fn run(&self) -> Result<Outcome, Self::Error> {

            self.trace.lock().await
                .push(self.index);

            if self.success {
                Ok(Outcome::Success)
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

    async fn trace(graph: TestGraph) -> (Vec<Option<usize>>, Option<Error<usize>>) {

        let trace = Mutex::new(vec![]);
        let job = TestJob {
            index: 0,
            graph: &graph,
            trace: &trace,
            success: graph[0].0,
        };

        let sched = Scheduler::default();
        let err = sched.run(job)
            .await
            .err();

        let mut results = vec![None; graph.len()];

        for (finished_idx, job_idx) in trace.into_inner().iter().enumerate() {

            // Ensure no job has had its update method called more than once
            assert!(results[*job_idx].is_none());

            results[*job_idx] = Some(finished_idx);
        }

        (results, err)
    }

    #[async_std::test]
    async fn single_job() {

        let (trace, err) = trace(vec![
            (true, vec![]),
        ]).await;

        assert!(err.is_none());
        assert_eq!(trace[0], Some(0));
    }

    #[async_std::test]
    async fn single_job_fails() {

        let (trace, err) = trace(vec![
            (false, vec![]),
        ]).await;

        assert_eq!(err, Some(Error::Fail(vec![0])));
        assert_eq!(trace[0], Some(0));
    }

    #[async_std::test]
    async fn single_dep() {

        let (trace, err) = trace(vec![
            (true, vec![1]),
            (true, vec![]),
        ]).await;

        assert!(err.is_none());
        assert_eq!(trace[0], Some(1));
        assert_eq!(trace[1], Some(0));
    }

    #[async_std::test]
    async fn single_dep_fails() {

        let (trace, err) = trace(vec![
            (true, vec![1]),
            (false, vec![]),
        ]).await;

        assert_eq!(err, Some(Error::Fail(vec![1])));
        assert_eq!(trace[0], None);
        assert_eq!(trace[1], Some(0));
    }

    #[async_std::test]
    async fn single_dep_root_fails() {

        let (trace, err) = trace(vec![
            (false, vec![1]),
            (true, vec![]),
        ]).await;

        assert_eq!(err, Some(Error::Fail(vec![0])));
        assert_eq!(trace[0], Some(1));
        assert_eq!(trace[1], Some(0));
    }

    #[async_std::test]
    async fn two_deps() {

        let (trace, err) = trace(vec![
            (true, vec![1, 2]),
            (true, vec![]),
            (true, vec![]),
        ]).await;

        assert!(err.is_none());
        assert_eq!(trace[0], Some(2));
        assert!(matches!(trace[1], Some(x) if x < 2));
        assert!(matches!(trace[2], Some(x) if x < 2));
    }

    #[async_std::test]
    async fn two_deps_one_fails() {

        let (trace, err) = trace(vec![
            (true, vec![1, 2]),
            (true, vec![]),
            (false, vec![]),
        ]).await;

        assert_eq!(err, Some(Error::Fail(vec![2])));
        assert_eq!(trace[0], None);
        // job 1 may or may not be updated
        assert!(trace[2].is_some());
    }

    #[async_std::test]
    async fn single_trans_dep() {

        let (trace, err) = trace(vec![
            (true, vec![1]),
            (true, vec![2]),
            (true, vec![]),
        ]).await;

        assert!(err.is_none());
        assert_eq!(trace[0], Some(2));
        assert_eq!(trace[1], Some(1));
        assert_eq!(trace[2], Some(0));
    }

    #[async_std::test]
    async fn single_trans_dep_fails() {

        let (trace, err) = trace(vec![
            (true, vec![1]),
            (true, vec![2]),
            (false, vec![]),
        ]).await;

        assert_eq!(err, Some(Error::Fail(vec![2])));
        assert_eq!(trace[0], None);
        assert_eq!(trace[1], None);
        assert_eq!(trace[2], Some(0));
    }

    #[async_std::test]
    async fn single_trans_dep_direct_dep_fails() {

        let (trace, err) = trace(vec![
            (true, vec![1]),
            (false, vec![2]),
            (true, vec![]),
        ]).await;

        assert_eq!(err, Some(Error::Fail(vec![1])));
        assert_eq!(trace[0], None);
        assert_eq!(trace[1], Some(1));
        assert_eq!(trace[2], Some(0));
    }

    #[async_std::test]
    async fn two_deps_single_trans_dep() {

        let (trace, err) = trace(vec![
            (true, vec![1, 3]),
            (true, vec![2]),
            (true, vec![]),
            (true, vec![]),
        ]).await;

        assert!(err.is_none());
        assert_eq!(trace[0], Some(3));
        assert!(matches!(trace[3], Some(x) if x < 3));

        let order_of_1 = trace[1].unwrap();
        let order_of_2 = trace[2].unwrap();
        assert!(order_of_1 > order_of_2);
        assert!(order_of_1 < 3);
    }

    #[async_std::test]
    async fn two_deps_each_with_trans_dep() {

        let (trace, err) = trace(vec![
            (true, vec![1, 3]),
            (true, vec![2]),
            (true, vec![]),
            (true, vec![4]),
            (true, vec![]),
        ]).await;

        assert!(err.is_none());
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

        let (trace, err) = trace(vec![
            (true, vec![1, 2, 3]),
            (true, vec![]),
            (true, vec![]),
            (true, vec![]),
        ]).await;

        assert!(err.is_none());
        assert_eq!(trace[0], Some(3));
        assert!(matches!(trace[1], Some(x) if x < 3));
        assert!(matches!(trace[2], Some(x) if x < 3));
        assert!(matches!(trace[3], Some(x) if x < 3));
    }

    #[async_std::test]
    async fn diamond() {

        let (trace, err) = trace(vec![
            (true, vec![2, 3]),
            (true, vec![]),
            (true, vec![1]),
            (true, vec![1]),
        ]).await;

        assert!(err.is_none());
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

        let (trace, err) = trace(vec![
            (true, vec![2, 3]),
            (true, vec![4]),
            (true, vec![1, 5]),
            (true, vec![1, 6]),
            (true, vec![]),
            (true, vec![]),
            (true, vec![]),
        ]).await;

        assert!(err.is_none());
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

    #[async_std::test]
    async fn simple_cycle() {

        let (trace, err) = trace(vec![
            (true, vec![1]),
            (true, vec![0]),
        ]).await;

        assert_eq!(err, Some(Error::Cycle));
        for job in trace {
            assert_eq!(job, None);
        }
    }

    #[async_std::test]
    async fn complex_cycle() {

        let (trace, err) = trace(vec![
            (true, vec![1, 2]),
            (true, vec![3]),
            (true, vec![1]),
            (true, vec![2]),
        ]).await;

        assert_eq!(err, Some(Error::Cycle));
        for job in trace {
            assert_eq!(job, None);
        }
    }
}
