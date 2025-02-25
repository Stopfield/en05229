# Aplicação Com Barreiras Aninhadas Restritas e Reutilizáveis

import logging
import multiprocessing
import random
import sys
import time
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from barrier.double import DoubleBarrier

NUM_PROCESSOS = 6 # Tem que ser mais do que o suportado
PORTA_DO_RESTAURANTE = "/porta_do_restaurante"
MESA_DE_JANTAR = "/mesa_de_jantar"

# Verifica se "debug" foi passado como argumento
debug_mode = "--debug" in sys.argv

# Configuração do logging
logging.basicConfig(
    level=logging.FATAL if debug_mode else logging.CRITICAL + 1,
    format="%(levelname)s - %(message)s",
)

def refresh_zk_environment():
    """Remove as barreiras para refazer o teste"""
    zk = KazooClient(hosts="127.0.0.1:2181")
    zk.start()
    
    zk.delete("/porta_do_restaurante", recursive=True)
    zk.delete("/mesa_de_jantar", recursive=True)
    
    zk.stop()
    

def philosopher(name: str):
    zk = KazooClient(hosts="127.0.0.1:2181")
    zk.start()
    
    # Delay aleatório
    random_client_delay = random.randint(0, 10)
    time.sleep(random_client_delay)

    # Entra no Restaurante
    porta_do_restaurante = DoubleBarrier(zk, "/porta_do_restaurante", 6)
    porta_do_restaurante.barrier_enter(name)
    time.sleep(2)

    # Tenta entrar na mesa
    mesa = DoubleBarrier(zk, "/mesa_de_jantar", 3)

    mesa.barrier_enter(name)
    time.sleep(2)
    mesa.barrier_leave(name)
    
    # Saiu da Mesa e agora tenta sair do Restaurante
    porta_do_restaurante.barrier_leave(name)
    
    zk.stop()
    
def watcher():
    zk = KazooClient(hosts="127.0.0.1:2181")
    zk.start()
    
    barrier = DoubleBarrier(zk, "/porta_do_restaurante", 6)
    table_barrier = DoubleBarrier(zk, "/mesa_de_jantar", 3)
    
    print("\033[2J", end="", flush="")
    
    @zk.ChildrenWatch(PORTA_DO_RESTAURANTE)
    def watch_porta_do_restaurante(children):
        print("\033[1;0f", end="", flush=True)
        print("\033[2K", end="")        
        print(f"\rDentro do Restaurante: {children}", end="")

    @zk.ChildrenWatch(MESA_DE_JANTAR)
    def watch_mesa_de_jantar(children):
        print("\033[2;0f", end="", flush=True)
        print("\033[2K", end="")
        print(f"\rNa mesa de jantar: {children}", end="")
        
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Finalizando watcher...")
        zk.stop()
        zk.close()

if __name__ == "__main__":
    refresh_zk_environment()
    
    # Vamos criar vários processos, para vários filósofos
    processes = []
    for i in range(NUM_PROCESSOS):
        p = multiprocessing.Process(target=philosopher, args=(f"Processo-{i + 1}",))
        processes.append(p)
    
    # Adiciona o watcher pra mostrar bonitinho
    if not debug_mode:
        processes.append(multiprocessing.Process(target=watcher, args=()))
    
    # Start all processes
    for p in processes:
        p.start()
    
    for p in processes:
        p.join()    
    
    
