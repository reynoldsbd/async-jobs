use async_trait::async_trait;

#[async_trait]
pub trait Target: Sized + PartialEq {

    type Error;

    fn dependencies(&self) -> Vec<Self>;

    async fn update(&self) -> Result<(), Self::Error>;
}

/// Unit of work needed to update a single target
///
/// The scheduler represents each target in the dependency graph using a separate
/// instance of `Job`. In addition to providing storage for the target itself, this
/// structure holds bookkeeping information such as dependency relationships.
struct Job<T>
where
    T: Target
{
    /// Target associated with this job
    target: T,
}

impl<T> From<T> for Job<T>
where
    T: Target
{
    fn from(target: T) -> Self {
        Job { target }
    }
}

/// A sequence of jobs needed to update a specific target
struct Schedule<T>
where
    T: Target
{
    /// List of jobs which comprise this schedule
    jobs: Vec<Job<T>>,

    /// List of indices (into `self.jobs`) in order of execution
    queue: Vec<usize>,
}

impl<T> Schedule<T>
where
    T: Target
{
    /// Creates a new job schedule which will update `target`
    fn new(target: T) -> Self {

        let mut sched = Self {
            jobs: Default::default(),
            queue: Default::default(),
        };

        sched.add_job(target.into());

        sched
    }

    /// Adds a job to this schedule
    fn add_job(&mut self, job: Job<T>) -> usize {

        let job_idx = self.jobs.len();
        self.jobs.push(job);

        for dep in self.jobs[job_idx].target.dependencies() {

            if !self.jobs.iter().any(|j| j.target == dep) {
                self.add_job(dep.into());
            }
        }

        self.queue.push(job_idx);
        job_idx
    }
}

pub struct Scheduler(());

impl Scheduler {

    pub fn new() -> Self {
        Scheduler(())
    }

    pub async fn update_target<T>(&self, target: T) -> Result<(), T::Error>
    where
        T: Target
    {
        let sched = Schedule::new(target);

        for i in sched.queue {
            sched.jobs[i].target.update().await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use async_std::sync::Mutex;

    use super::*;

    type TestGraph = Vec<(bool, Vec<usize>)>;

    struct TestTarget<'a> {
        index: usize,
        graph: &'a TestGraph,
        trace: &'a Mutex<Vec<usize>>,
        success: bool,
    }

    #[async_trait]
    impl<'a> Target for TestTarget<'a> {

        type Error = usize;

        fn dependencies(&self) -> Vec<Self> {

            let mut deps = vec![];

            for index in &self.graph[self.index].1 {
                deps.push(TestTarget {
                    index: *index,
                    graph: self.graph,
                    trace: self.trace,
                    success: self.graph[*index].0,
                })
            }

            deps
        }

        async fn update(&self) -> Result<(), Self::Error> {
            self.trace.lock().await
                .push(self.index);

            if self.success {
                Ok(())
            } else {
                Err(self.index)
            }
        }
    }

    impl<'a> PartialEq for TestTarget<'a> {
        fn eq(&self, other: &Self) -> bool {
            self.index == other.index
        }
    }

    async fn trace(graph: TestGraph) -> (Vec<Option<usize>>, Option<usize>) {

        let trace = Mutex::new(vec![]);
        let target = TestTarget {
            index: 0,
            graph: &graph,
            trace: &trace,
            success: graph[0].0,
        };

        let sched = Scheduler::new();
        let failed_idx = sched.update_target(target)
            .await
            .err();

        let mut results = vec![None; graph.len()];

        for (finished_idx, target_idx) in trace.into_inner().iter().enumerate() {

            // Ensure no target has had its update method called more than once
            assert!(results[*target_idx].is_none());

            results[*target_idx] = Some(finished_idx);
        }

        (results, failed_idx)
    }

    #[async_std::test]
    async fn single_target() {

        let (trace, failed) = trace(vec![
            (true, vec![]),
        ]).await;

        assert!(failed.is_none());
        assert_eq!(trace[0], Some(0));
    }

    #[async_std::test]
    async fn single_target_fails() {

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
        // target 1 may or may not be updated
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
