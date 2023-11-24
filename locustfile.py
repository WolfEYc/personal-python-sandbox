from locust import HttpUser, task


class MyUser(HttpUser):
    @task
    def my_task(self):
        self.client.get("/purchase?page=0")
        self.client.get("/account?page=0")
