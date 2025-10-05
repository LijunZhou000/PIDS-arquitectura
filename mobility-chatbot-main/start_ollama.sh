#!/bin/bash

# Check if gemma:2b model is already installed
if ! ollama list | grep -q "gemma:2b"; then
  echo "Gemma:2b model not found, downloading..."
  ollama pull gemma:2b
else
  echo "Gemma:2b model already installed."
fi

# Start Ollama service (replace this with the actual command to start the service)
ollama run
