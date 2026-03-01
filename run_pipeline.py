import subprocess
import sys
from datetime import datetime

def run_step(script, descricao):
    print(f"\n{'='*50}")
    print(f"INICIANDO: {descricao}")
    print(f"{'='*50}")

    result = subprocess.run(
        [sys.executable, script],
        capture_output=False
    )

    if result.returncode != 0:
        print(f"\nERRO em {descricao}. Pipeline interrompido.")
        sys.exit(1)

    print(f"CONCLUÍDO: {descricao}")

def main():
    inicio = datetime.now()
    print(f"\nPIPELINE AML — Iniciado em {inicio.strftime('%Y-%m-%d %H:%M:%S')}")

    run_step("pipeline/01_load_bronze.py",      "Carga Bronze — CSV → PostgreSQL")
    run_step("pipeline/02_transform_silver.py", "Transformação Silver — limpeza e tipagem")
    run_step("pipeline/03_transform_gold.py",   "Transformação Gold — classificação de risco")

    fim = datetime.now()
    duracao = (fim - inicio).seconds
    print(f"\n{'='*50}")
    print(f"PIPELINE CONCLUÍDO em {duracao} segundos")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()