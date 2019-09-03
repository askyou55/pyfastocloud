from abc import ABC, abstractmethod

from pyfastocloud.json_rpc import Request, Response
from pyfastocloud.client_constants import ClientStatus


# handler for client
class IClientHandler(ABC):
    @abstractmethod
    def process_response(self, client, req: Request, resp: Response):
        pass

    @abstractmethod
    def process_request(self, client, req: Request):
        pass

    @abstractmethod
    def on_client_state_changed(self, client, status: ClientStatus):
        pass
