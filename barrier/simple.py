from kazoo.recipe.barrier import Barrier


class SBarrier:
    def __init__(self, client, path):
        self.barrier = Barrier(client, path)

    def wait(self):
        self.barrier.wait()

    def remove(self):
        self.barrier.remove()
