import schedule
import time
import subprocess


def executar_codigo():
    code_main = r"C:\Users\nayya\Downloads\Estudo\projetos\desafio-beAnalytic-engdadosjr\scripts\main.py"
    subprocess.call(code_main, shell=True)


# Agendar a execução diária às 9:00 da manhã
schedule.every().day.at("09:00").do(executar_codigo)

# Loop para verificar se há tarefas agendadas e executá-las
while True:
    schedule.run_pending()
    time.sleep(1)
