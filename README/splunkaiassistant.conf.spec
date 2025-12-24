[splunk_ai_assistant]
* configuration for splunk ai assistant

[cloud_connected_configurations]
* configuration related to SAIA cloud connected

tenant_name = <string>
* Tenant name provided to the splunk customer(tenant)

tenant_hostname = <string>
* Tenant hostname - The FQDN used in invoking the API call to SCS services

scs_region = <string>
* SCS region to which the cloud-connected tenant is onboarded to

service_principal = <string>
* Service principal attached to the onboarded tenant
* Used to invoke API calls to SCS scoped to specific services

scs_token = <string>
* SCS access token used to authenticate the API calls made to SCS service (specifically saia-service)
* Generated using service_principal

scs_token_expiry = <integer>
* Expiry timestamp of the scs_token
* This is valid for 60 minutes and so is rotated for every 60 minutes

last_setup_timestamp = <integer>
* Timestamp of SAIA cloud connected setup time
* The rotates every 85-90 days as per maintenance workflow code

encoded_onboarding_data = <string>
* Encoded string of onboarding details used for Activation


[cloud_connected_configurations:proxy_settings]

proxy_enabled = <integer>
* Flag for denoting proxy is enabled or disabled

proxy_type = <string>
* Scheme used by the Proxy server

proxy_hostname = <string>
* Hostname of the Proxy server

proxy_port = <string>
* Port of the Proxy server

proxy_auth_enabled = <string>
* Flag for denoting auth for proxy is enabled or disabled

proxy_username = <string>
* Username to connect the Proxy server if authentication is enabled
