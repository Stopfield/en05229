import multiprocessing
import time

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError


class DoubleBarrier:
    def __init__(self, client: KazooClient, path: str, condition):
        self.client = client
        self.path = path
        self.condition = condition
        self.ready_path = f"{self.path}/ready"

        self.create()

    def create(self):
        self.client.ensure_path(self.path)

        # Se o nó "ready" existir antes de começarmos, removemos para evitar bloqueios
        if self.client.exists(self.ready_path):
            print("🔄 Resetando barreira para o novo ciclo...")
            try:
                self.client.delete(self.ready_path)
            except NoNodeError:
                pass  # Se outro processo já removeu, seguimos normalmente

        print("✅ A barreira está pronta para novos processos!")

    def barrier_enter(self, name):
        try:
            self.client.create(f"{self.path}/{name}", ephemeral=True)
        except NodeExistsError:
            pass

        while True:
            children = self.client.get_children(self.path)
            print(f"{name} vê {len(children)} processos na barreira: {children}")
            if len(children) >= self.condition:
                try:
                    self.client.create(self.ready_path, ephemeral=False)
                except NodeExistsError:
                    pass  # Se já existe, seguimos normalmente
                break
            time.sleep(1)

    def barrier_leave(self, name):
        try:
            self.client.delete(f"{self.path}/{name}")
        except NoNodeError:
            pass

        while True:
            children = self.client.get_children(self.path)
            print(
                f"{name} vê {len(children)} processos restantes na barreira: {children}"
            )
            if len(children) == 1 and "ready" in children:
                # Se somos o último processo a sair, removemos a barreira
                try:
                    self.client.delete(self.ready_path)
                    print(
                        "🔄 A barreira foi resetada e está pronta para novos processos!"
                    )
                except NoNodeError:
                    pass  # Se já foi removido por outro processo, ignoramos
                break
            elif len(children) == 0:
                break
            time.sleep(1)


def init_zookeeper():
    zk = KazooClient(hosts="127.0.0.1:2181")
    zk.start()
    return zk


def philosopher(name):
    zk = init_zookeeper()
    barrier = DoubleBarrier(zk, "/barreira", 5)

    print(f"{name} está executando...")
    time.sleep(2)

    print(f"{name} está tentando entrar na barreira...")
    barrier.barrier_enter(name)
    print(f"{name} entrou na barreira!")
    time.sleep(2)

    print(f"{name} está processando...")
    time.sleep(2)

    print(f"{name} está tentando sair da barreira...")
    barrier.barrier_leave(name)
    print(f"{name} saiu da barreira e pode reiniciar.")

    zk.stop()


if __name__ == "__main__":
    NUM_CICLOS = 3
    NUM_PROCESSOS = 5

    for ciclo in range(NUM_CICLOS):  # Executa múltiplos ciclos de sincronização
        print(f"\n🚀 **Iniciando ciclo {ciclo + 1} da barreira** 🚀\n")
        processes = []

        for i in range(NUM_PROCESSOS):
            p = multiprocessing.Process(target=philosopher, args=(f"Processo-{i + 1}",))
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

        print(f"✅ **Ciclo {ciclo + 1} concluído!**\n")

    print("🎉 Todos os ciclos da barreira foram concluídos com sucesso!")
