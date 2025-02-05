from kazoo.recipe.barrier import DoubleBarrier


class DBarrier:
    def __init__(self, client, path, n_clients):
        self.barrier = DoubleBarrier(client, path, n_clients)

    def enter(self):
        self.barrier.enter()

    def leave(self):
        self.barrier.leave()
