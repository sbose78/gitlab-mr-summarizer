from framework.interfaces import ContextGenerator
from typing import Callable, Dict

class MRChangesContextGenerator(ContextGenerator):
    def __init__(self, get_mr_changes_func: Callable, project_id: str, mr_iid: str, token: str):
        self.get_mr_changes_func = get_mr_changes_func
        self.project_id = project_id
        self.mr_iid = mr_iid
        self.token = token

    def requires(self) -> set:
        return set()

    def provides(self) -> set:
        return {'mr_changes'}

    def generate(self, context: Dict) -> None:
        context['mr_changes'] = self.get_mr_changes_func(
            self.project_id,
            self.mr_iid,
            self.token
        ) 