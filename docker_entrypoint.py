#!/usr/bin/env python3
"""
Docker Entrypoint para ETL Pipeline Silver Lakehouse
Compatível com Windows e Linux
"""

import sys
import time
import subprocess
import os
from pathlib import Path


def print_step(message: str, emoji: str = "🔹"):
    """Imprime mensagem formatada"""
    print(f"{emoji} {message}")
    sys.stdout.flush()


def test_postgres_connection(max_retries: int = 30, retry_delay: int = 2) -> bool:
    """
    Testa conexão com PostgreSQL com retries
    """
    print_step("Testando conexão com PostgreSQL...", "🔍")

    import psycopg2

    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host="postgres",
                port=5432,
                user="admin",
                password="admin123",
                database="sinistros_prf",
                connect_timeout=5,
            )
            conn.close()
            print_step("PostgreSQL está pronto!", "✅")
            return True

        except psycopg2.OperationalError as e:
            if attempt < max_retries:
                print_step(
                    f"Tentativa {attempt}/{max_retries} falhou. "
                    f"Aguardando {retry_delay}s...",
                    "⏳",
                )
                time.sleep(retry_delay)
            else:
                print_step(
                    f"Não foi possível conectar ao PostgreSQL após {max_retries} tentativas",
                    "❌",
                )
                print_step(f"Erro: {e}", "⚠️")
                return False
        except Exception as e:
            print_step(f"Erro inesperado: {e}", "❌")
            return False

    return False


def run_etl_pipeline() -> int:
    """
    Executa o pipeline ETL
    """
    print_step("Executando Pipeline ETL...", "🔄")

    # Configurar PYTHONPATH
    pythonpath = os.environ.get("PYTHONPATH", "")
    os.environ["PYTHONPATH"] = f"/usr/src:{pythonpath}"

    # Caminho do script pipeline
    pipeline_script = Path("/usr/src/silver/etl/jobs/pipeline.py")

    if not pipeline_script.exists():
        print_step(f"Script pipeline não encontrado: {pipeline_script}", "❌")
        return 1

    # Executar pipeline
    try:
        result = subprocess.run(
            [sys.executable, str(pipeline_script)],
            cwd=str(pipeline_script.parent),
            env=os.environ.copy(),
            capture_output=False,
            text=True,
        )

        if result.returncode == 0:
            print_step("Pipeline ETL concluído com sucesso!", "🎉")
            return 0
        else:
            print_step(f"Pipeline ETL falhou com código: {result.returncode}", "❌")
            return result.returncode

    except Exception as e:
        print_step(f"Erro ao executar pipeline: {e}", "❌")
        return 1


def main():
    """Função principal do entrypoint"""
    print_step("Iniciando ETL Pipeline Silver Lakehouse...", "🚀")
    print()

    # 1. Aguardar e testar PostgreSQL
    print_step("Aguardando PostgreSQL inicializar...", "⏳")

    if not test_postgres_connection():
        print_step("Continuando mesmo sem conexão com PostgreSQL...", "⚠️")

    print()

    # 2. Executar pipeline ETL
    exit_code = run_etl_pipeline()

    print()

    # 3. Verificar modo keep-alive
    keep_alive = os.environ.get("KEEP_ALIVE", "false").lower() == "true"

    if keep_alive:
        print_step("Container em modo de espera...", "💤")
        try:
            # Mantém o container rodando indefinidamente
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            print_step("Container encerrado pelo usuário", "👋")
            sys.exit(0)
    else:
        # Encerrar com código apropriado
        if exit_code == 0:
            print_step("Entrypoint concluído com sucesso!", "✅")
        else:
            print_step("Entrypoint finalizado com erros", "⚠️")

        sys.exit(exit_code)


if __name__ == "__main__":
    main()
