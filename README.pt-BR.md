# Vyane

**Execute agentes de programação e APIs de modelos por uma única CLI em Rust.**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

Para desenvolvedores que usam mais de um agente de programação ou provedor de modelos: dê nomes aos destinos, envie uma tarefa para um ou vários deles e mantenha os resultados e o histórico de tentativas juntos. Use Claude Code ou Codex CLI para programar, ou chame uma API de modelos diretamente para obter respostas em texto.

> **Desenvolvimento inicial.** Instalação apenas pelo código-fonte; as APIs são instáveis e algumas capacidades continuam parciais ou ausentes. A retomada de sessões nativas e a reexecução automática após reiniciar ainda não estão prontas. Veja os [limites atuais](docs/PROJECT-STATUS.md).

## Veja em funcionamento

[Execute a demonstração no terminal](docs/EXAMPLES.md): a CLI instalada executa uma tarefa, envia para dois destinos em paralelo e se recupera de uma falha programada no destino principal. A demonstração usa respostas HTTP de teste locais, não precisa de chaves e não chama modelos reais.

```text
Local HTTP fixtures only; no live model calls.
dispatch: fixture: chat-model; recorded attempts=1
broadcast: 2 separate successful results (chat-model, review-model)
failover: fixture: review-model; recorded attempts=2
```

## Três formas de usar

| Quando você quiser… | Use | O que recebe |
| --- | --- | --- |
| Executar uma tarefa com um agente ou modelo escolhido | [`dispatch --target chat`](docs/EXAMPLES.md#one-task-one-target) | Um resultado e um registro local. Os adaptadores de agentes precisam de instalação e configuração separadas. |
| Comparar respostas ao mesmo prompt | [`broadcast --targets chat,review`](docs/EXAMPLES.md#one-prompt-several-targets) | Execuções simultâneas com resultados separados; você avalia as respostas. |
| Tentar um destino reserva configurado quando uma chamada falhar | [`dispatch --target resilient`](docs/EXAMPLES.md#an-explicit-backup) | Uma cadeia ordenada de failover para erros elegíveis, com cada tentativa registrada. Isso não transfere a sessão. |

## Início rápido

**1. Instale pelo código-fonte.** É necessário Rust/Cargo estável (no mínimo Rust 1.88) e ferramentas de compilação C/C++. A CI cobre Linux e macOS; não declaramos suporte a Windows.

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
```

Versão esperada: `vyane 0.1.0`. O binário fica nesta cópia do repositório, sem substituir outro `vyane` no PATH. Mantenha este shell aberto.

**2. Obtenha um primeiro resultado sem chaves.** O exemplo incluído apenas mostra uma prévia do roteamento; não envia solicitações a modelos.

```sh
"$VYANE" --config docs/examples/offline.toml check
"$VYANE" --config docs/examples/offline.toml route "Explain this function" --json
```

Procure o perfil `offline` e `"selection_key": "offline"` no JSON. O endpoint é inválido de propósito; não envie tarefas para ele.

**3. Conecte seu próprio destino.** Siga a [configuração do provedor](docs/QUICKSTART.md#configure-a-real-target) e experimente os [três exemplos](docs/EXAMPLES.md). Você fornece acesso ao modelo e credenciais; chamadas reais podem gerar cobranças do provedor. A prévia offline não valida um provedor real.

## O que funciona hoje

| Disponível no código-fonte | Limite |
| --- | --- |
| Execução individual, envio paralelo, failover explícito e streaming | HTTP direto retorna texto do modelo; não anexa arquivos locais nem fornece ferramentas de shell. |
| Adaptadores para Claude Code e Codex CLI | Subprocessos CLI, sem Codex App Server RPC nem integração direta com Claude Agent SDK. Você fornece a CLI e o acesso; o adaptador aplica seu sandbox. |
| Roteamento por perfis e histórico local | O roteamento segue sua configuração; não garante a qualidade das respostas. |
| Entradas CLI, REST e MCP | Cada uma expõe um subconjunto diferente. Veja o [guia da CLI](docs/CLI-GUIDE.md). |

Execução residente, workflows persistentes e objetivos locais continuam em evolução. Execuções nativas começam do zero e não mantêm sessão; a retomada de aprovações e sessões nativas e a reexecução automática após reiniciar continuam pendentes. Campos de proprietário e autenticação de loopback não isolam acessos hostis do mesmo usuário ou de múltiplos usuários. Leia o [estado e as limitações](docs/PROJECT-STATUS.md) antes de desenvolver sobre essas funções.

## Saiba mais

- [Arquitetura e modelo de destinos](docs/ARCHITECTURE.md) · [Referência de configuração](profiles.example.toml) · [Guia de CLI, REST e MCP](docs/CLI-GUIDE.md)
- [Matriz de capacidades e evidências reproduzíveis](docs/PROJECT-STATUS.md#relationship-to-python-vyane) · [Roteiro](docs/ROADMAP.md)
- [Histórico de mudanças](docs/CHANGELOG.md) · [Política de versões e lançamentos](docs/RELEASING.md)

Esta é a implementação pública em Rust de capacidades do sistema privado Python Vyane original. Não é uma substituição direta: comandos, configuração, sessões salvas e workflows precisam de verificações individuais de migração. Não compartilhe diretórios de estado sem um procedimento documentado. Colaboradores públicos não precisam acessar o código privado.

## Contribua e peça ajuda

Experimente um exemplo, relate um problema reproduzível, melhore uma tradução ou resolva uma lacuna documentada. Confira as [Issues](https://github.com/zleo-ai/vyane-rs/issues) e os [PRs abertos](https://github.com/zleo-ai/vyane-rs/pulls), depois leia [CONTRIBUTING.md](CONTRIBUTING.md). Inclua a revisão do código e uma saída sem dados sensíveis; omita chaves, prompts privados e caminhos pessoais. Relate vulnerabilidades seguindo [SECURITY.md](SECURITY.md).

## Licença

[MIT](LICENSE-MIT) ou [Apache-2.0](LICENSE-APACHE), à sua escolha. As contribuições seguem os mesmos termos.

---

Base do código: `fe43941` · Workspace: `0.1.0` · Atualizado: 2026-10-15. O inglês é a referência; esta tradução assistida por máquina aguarda revisão humana por falantes nativos. Veja a [cobertura e o estado das traduções](docs/TRANSLATING.md). Marcos do roteiro não são versões publicadas.
