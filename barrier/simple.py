from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
import logging
import time
import multiprocessing

# Configuração de logging


# Configuração Zookeeper
# TODO: Colocar no Main

class SimpleBarrier:
    """Barreira simples com função barrier_wait"""
    def __init__(self, client, path):
        # TODO: Definir condição de saída
        self.client = client
        self.path = path

    def create(self):
        """Cria a barreira no Zookeeper"""
        self.client.ensure_path(self.path)

    def remove(self):
        """Remove a barreira e libera os processos"""
        try:
            self.client.delete(self.path)
            logging.info("🚪 A barreira foi removida. Todos podem continuar!")
        except NoNodeError:
            logging.warning("Barreira já removida!")

    def wait(self):
        """Função barrier_wait() simplificada: espera todos os processos chegarem"""
        # TODO: Colocar multiprocessing só no lado cliente
        # TODO: O Cliente só deve chamar `wait()`, não o `create()`. Verificar se não existir, criar.
        event = multiprocessing.Event()

        def watch_event(event_data):
            """Callback acionado quando a barreira é removida"""
            if event_data.type == "DELETED":
                event.set()

        # Se a barreira não existe, retorna imediatamente
        if not self.client.exists(self.path, watch=watch_event):
            return True

        logging.info(f"🥄 Filósofo {multiprocessing.current_process().name} esperando na barreira...")
        event.wait()
        return event.is_set()



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    def filosofo():
        """Simula filósofos esperando e comendo"""
        client = KazooClient(hosts=ZK_ADDRESS)
        client.start()

        barrier = SimpleBarrier(client, BARRIER_PATH)
        barrier.wait()  # Espera pela remoção da barreira

        print(f"🍷 Filósofo {multiprocessing.current_process().name} começou a comer!")
        time.sleep(2)  # Simula tempo de refeição
        print(f"✅ Filósofo {multiprocessing.current_process().name} terminou de comer!")

        client.stop()
    
    ZK_ADDRESS = "127.0.0.1:2181"
    BARRIER_PATH = "/barreira_filosofos"
    
    client = KazooClient(hosts=ZK_ADDRESS)
    client.start()

    barrier = SimpleBarrier(client, BARRIER_PATH)
    barrier.create()  # Criação da barreira

    # Criando e iniciando os filósofos como processos
    filosofos = [multiprocessing.Process(target=filosofo, name=f"Filósofo-{i}") for i in range(5)]
    for f in filosofos:
        f.start()

    time.sleep(3)  # Aguarda para simular um tempo de espera antes de remover a barreira
    print("🔔 Todos os filósofos podem começar a comer!")
    barrier.remove()  # Remove a barreira, liberando os filósofos

    # Aguarda todos os filósofos terminarem
    for f in filosofos:
        f.join()

    client.stop()
