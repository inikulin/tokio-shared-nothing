use async_channel::{Receiver, Sender};
use std::future::Future;
use std::thread;

pub fn block_on<Task, Out, MainFut, WorkerFut>(
    main: impl FnOnce(Sender<Task>) -> MainFut,
    worker: impl Fn(Receiver<Task>) -> WorkerFut,
) -> Out
where
    MainFut: Future<Output = Out>,
    WorkerFut: Future<Output = Out>,
{
    todo!()
}
