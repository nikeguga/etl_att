FROM apache/airflow:2.5.1

# Удаляем несовместимую версию SQLAlchemy и ставим корректную
RUN pip uninstall -y sqlalchemy && \
    pip install 'sqlalchemy<2.0'
