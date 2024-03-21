#!/bin/bash

# Script para instalar as dependências de um módulo a partir do arquivo requirements.txt
# Apenas use este script dentro do DevContainer Spark ou Python Shell

# Verifica se ao menos dois argumentos foram fornecidos (identificador + pelo menos um módulo)
if [ "$#" -lt 1 ]; then
  echo "Uso: $0 {spark|python_shell}"
  exit 1
fi

# Armazena o primeiro parâmetro e remove-o da lista de parâmetros
IDENTIFIER=$1
shift

# Verifica o identificador
if [ "$IDENTIFIER" != "spark" ] && [ "$IDENTIFIER" != "python_shell" ]; then
  echo "O primeiro parâmetro deve ser 'spark' ou 'python_shell'."
  exit 1
fi

if [ "$IDENTIFIER" = "spark" ]; then
  python3 -m pip install --force-reinstall --no-deps -r ./src/spark/requirements.txt
fi

if [ "$IDENTIFIER" = "python_shell" ]; then
  python3 -m pip install --force-reinstall --no-deps -r ./src/python_shell/requirements.txt
fi