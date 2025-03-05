use async_trait::async_trait;
use common::{TaskResult, WorkerId, WorkflowRunTask};
use uuid::Uuid;

pub struct Worker {
    worker_id: WorkerId,
    executors: Vec<Box<dyn WorkflowTaskExecutor>>,
}

impl Worker {
    pub async fn run(self) {
        // Add code to connect to the workflow driver and pull down tasks to execute
    }
}

#[derive(Default)]
pub struct WorkerBuilder {
    executors: Vec<Box<dyn WorkflowTaskExecutor>>,
}

impl WorkerBuilder {
    pub fn add_executor<E: WorkflowTaskExecutor + 'static>(
        &mut self,
        workflow_task_executor: E,
    ) -> &mut Self {
        self.executors.push(Box::new(workflow_task_executor));
        self
    }

    pub fn build(self) -> Worker {
        Worker {
            worker_id: WorkerId::from(Uuid::now_v7()),
            executors: self.executors
        }
    }
}

#[async_trait]
pub trait WorkflowTaskExecutor {
    async fn execute(&self, worker: &Worker, workflow_task: WorkflowRunTask) -> TaskResult;
}
