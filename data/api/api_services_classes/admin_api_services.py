from data.data_common.dependencies.dependencies import tenants_repository, google_creds_repository


class AdminApiService:
    def __init__(self):
        self.tenants_repository = tenants_repository()
        self.google_creds_repo = google_creds_repository()
