import schedule
import time
from pipeline import executar_pipeline

# Agendamento do pipeline para rodar a cada 5 minutos
schedule.every(5).minutes.do(executar_pipeline)

# Loop para manter o agendamento ativo
if __name__ == '__main__':
    while True:
        # Executa as tarefas agendadas, caso seja o momento
        schedule.run_pending()

        # Aguarda um segundo antes de verificar novamente
        time.sleep(1)