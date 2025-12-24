import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
from splunklib.searchcommands import dispatch, GeneratingCommand, Configuration, Option
from splunklib.binding import HTTPError
from splunklib.client import Service


@Configuration()
class SetSCSInfoCommand(GeneratingCommand):
    api_key = Option(require=False)
    tenant = Option(require=False)
    tenant_hostname = Option(require=False)

    def get_passwd(self, name):
        service: Service = self.service # pyright: ignore , self.service can not be None
        try:
            return service.storage_passwords[
                    f"{name}:{self.metadata.searchinfo.username}" # pyright: ignore
            ]
        except KeyError:
            self.logger.info(f"no existing record found, creating {name}") # pyright: ignore
            return service.storage_passwords.create(
                password="none", username=self.metadata.searchinfo.username, realm=name # pyright: ignore
            )

    def generate(self):
        api_key_entry = self.get_passwd("api_key")
        tenant_entry = self.get_passwd("tenant")
        tenant_hostname_entry = self.get_passwd("tenant_hostname")

        try:
            if self.api_key:
                api_key_entry = api_key_entry.update(password=self.api_key)
            if self.tenant:
                tenant_entry = tenant_entry.update(password=self.tenant)
            if self.tenant_hostname:
                tenant_hostname_entry = tenant_hostname_entry.update(
                    password=self.tenant_hostname
                )
        except HTTPError as e:
            self.logger.error(e) # pyright: ignore
            raise RuntimeError(str(e))

        yield self.gen_record(
            tenant=tenant_entry.clear_password,
            api_key="[updated]",
            tenant_hostname=tenant_hostname_entry.clear_password,
        )

if __name__ == "__main__":
    dispatch(command_class=SetSCSInfoCommand)
