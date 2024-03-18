from typing import TypeVar, Protocol, Callable, Generic, runtime_checkable, Any, List

Context = TypeVar("Context", contravariant=True)
NextStep = Callable[[Any], None]


@runtime_checkable
class PipeStep(Protocol[Context]):
    def __call__(self, context: Context, next_step: NextStep) -> None: ...


class Pipeline(Generic[Context]):
    def __init__(self, *steps: PipeStep) -> None:
        self.queue = [step for step in steps]

    def append(self, step):
        self.queue.append(step)

    def __call__(self, context: Context):
        execute: PipeCursor = PipeCursor(self.queue)

        return execute(context)


class PipeCursor(Generic[Context]):
    def __init__(self, steps: List[PipeStep]):
        self.queue = steps

    def __call__(self, context: Context) -> None:
        if not self.queue:
            return None
        current_step = self.queue[0]
        next_step: PipeCursor = PipeCursor(self.queue[1:])

        current_step(context, next_step)


if __name__ == "__main__":
    ...

    # class TestContext(ContextInterface):
    #     def __init__(self) -> None:
    #         self.steps_history: List[str] = []
    #
    # class A:
    #     def __call__(self, context: TestContext, next_step: NextStep) -> None:
    #         context.do_step("A")
    #         print("executed step A", context.steps_history)
    #         next_step(context)
    #
    # a = A()
    # pipeline: Pipeline = Pipeline(a, a)
    # pipeline(TestContext())
    #
    # def step_a(context: TestContext, next_step: NextStep) -> None:
    #     context.steps_history.append("A")
    #     print("executed step A")
    #     next_step(context)
    #
    # pipeline = Pipeline(step_a)
    # pipeline(TestContext())
