class ResultAction:
    release = 0
    ack = 1
    delete = 2


class WorkResult:

    action: ResultAction
    delay: int              # for release

    def __init__(self, action: ResultAction = ResultAction.release, delay: int = 1):
        self.action = action
        self.delay = delay