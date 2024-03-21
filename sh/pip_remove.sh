#!/bin/bash

# Script para remover as dependências de um módulo a partir de uma lista de módulos
# Apenas use este script dentro do DevContainer Spark ou Python Shell

# Verifica se ao menos dois argumentos foram fornecidos (identificador + pelo menos um módulo)
if [ "$#" -lt 2 ]; then
  echo "Uso: $0 {spark|python_shell} modulo1 [modulo2 ...]"
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

python3 -m pip remove "$@"

# Atualiza o arquivo de dependências de acordo com o identificador
if [ "$IDENTIFIER" = "spark" ]; then
  python3 -m pip freeze > ./src/spark/requirements.txt
fi

if [ "$IDENTIFIER" = "python_shell" ]; then
  python3 -m pip freeze > ./src/python_shell/requirements.txt
fi
