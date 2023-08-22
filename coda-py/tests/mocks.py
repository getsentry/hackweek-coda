import uuid

from src.coda.supervisor import SupervisorAPI, SupervisorRequest, _default_message_condition

# TODO: implement mock server.
class MockServer:

    pass

class MockSupervisorAPI(SupervisorAPI):

    def __init__(self):
        self.messages = [
            {
                "type": "req",
                "cmd": "execute_workflow",
                "args": {
                    "workflow_name": "MyWorkflow",
                    "workflow_run_id": uuid.UUID("fea61924-99f5-45f5-82c1-1082efeaa6af").bytes,
                    "params_id": uuid.UUID("fea61924-99f5-45f5-82c1-1082efeaa6af").bytes,
                },
            },
            {
                "type": "resp",
                "request_id": uuid.UUID("fea61924-99f5-45f5-82c1-1082efeaa6af").bytes,
                "result": 100
            }
        ]
        self.index = 0

    def make_request(self, cmd, args):
        return SupervisorRequest(
            cmd=cmd,
            request_id=uuid.UUID("fea61924-99f5-45f5-82c1-1082efeaa6af").bytes
        )

    def build_condition_for_response(self, request):
        return _default_message_condition(
            _type="resp",
            request_id=request.request_id
        )

    def get_next_message(self):
        if self.index >= len(self.messages):
            return None

        message = self.messages[self.index]
        self.index += 1
        return message

    def extract_response(self, response):
        return response["result"]