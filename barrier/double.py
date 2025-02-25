import multiprocessing
import random
import time
import threading
import logging

from kazoo.client import KazooClient, WatchedEvent, EventType
from kazoo.exceptions import NodeExistsError, NoNodeError


class DoubleBarrier:
    def __init__(self, client: KazooClient, path: str, condition):
        self.client = client
        self.path = path
        self.condition = condition
        self.ready_path = f"{self.path}/ready"
        self.lock = threading.Lock()

        self.create()

    def watch_ready_deletion(self, event: WatchedEvent):
        """Se o Ready for deletado, hora de entrar"""
        if event.type == EventType.DELETED:
            # fatal("Nó ready deletado, posso entrar")
            self.lock.release()

    def watch_children_limit(self, event: WatchedEvent):
        """Verifica o número de filhos no nó"""
        # fatal("Filhos mudaram na barreira!")
        if self.lock.locked():
            self.lock.release()

    def create(self):
        self.client.ensure_path(self.path)

    def barrier_enter(self, name):
        # Verifica se o ready foi criado ou se já bateu o limite na barreira
        # Se for, vamos esperar
        while True:
            time.sleep(random.randint(0, 5))
            ready_exists = self.client.exists(
                self.ready_path, watch=self.watch_ready_deletion
            )
            barrier_children = self.client.get_children(
                self.path, watch=self.watch_children_limit
            )
            if ready_exists or len(barrier_children) >= self.condition:
                self.lock.acquire()
            else:
                break

        try:
            while True:  
                if len(self.client.get_children(self.path)) < self.condition:
                    self.client.create(f"{self.path}/{name}", ephemeral=True)
                    break
                else:
                    self.lock.acquire()
        except NodeExistsError:
            pass

        while True:
            children = self.client.get_children(self.path)
            logging.fatal(
                f"{name} vê {len(children)} processos na barreira {self.path}: {children}"
            )
                
            
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
            logging.fatal(
                f"{name} vê {len(children)} processos restantes na barreira {self.path}: {children}"
            )
            if len(children) == 1 and "ready" in children:
                # Se somos o último processo a sair, removemos a barreira
                try:
                    self.client.delete(self.ready_path)
                    logging.fatal("Deletando ready node")
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
