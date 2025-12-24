
from splunklib.client import Service
from ..saia_tool_scratchpad import SaiaToolScratchpad

class ToolRequest():
    tool_id: str
    job_id: str
    chat_id: str
    user: str
    kwargs: dict
    service: Service
    system_scoped_service: Service
    logging_context: dict

    def __init__(self, tool_id, job_id, chat_id, user, kwargs, service, system_scoped_service, logging_context):
        self.tool_id=tool_id
        self.job_id=job_id
        self.chat_id=chat_id
        self.user=user
        self.kwargs=kwargs
        self.service=service
        self.system_scoped_service=system_scoped_service
        self.logging_context = logging_context
        self.scratchpad = SaiaToolScratchpad(service, username=user)