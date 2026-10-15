# Vyane

**Execute e coordene tarefas de modelos de IA a partir de uma única CLI e de um núcleo de orquestração em Rust.**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

Revisão de referência: `b46c194` · Versão do workspace: `0.1.0` · Atualizado em: 2026-10-15.
A versão em inglês é a referência oficial. Esta tradução é um rascunho assistido por máquina; não passou por revisão de falantes nativos. Consulte a [política de tradução](docs/TRANSLATING.md).

O Vyane envia uma tarefa a um destino configurado, distribui a tarefa para vários destinos ou tenta uma cadeia explícita de destinos alternativos em caso de falha. Os destinos podem usar os ambientes de execução do Claude Code ou do Codex CLI, ou chamar diretamente endpoints HTTP de modelos. O projeto é voltado a desenvolvedores que criam fluxos de trabalho locais com agentes e aplicações Rust que precisam de roteamento explícito, permissões de execução, cancelamento e registros de execução.

![Janelas abstratas de terminal se conectam a cartões de resultados por meio de um centro de coordenação em tom de cobre.](docs/assets/vyane-hero.webp)

*Ilustração conceitual gerada por IA; não é um diagrama de arquitetura. [Notas sobre o recurso](docs/assets/README.md).*

## Estado e limitações

Este projeto está em uma fase inicial, é compilado a partir do código-fonte e tem APIs instáveis. A versão do Cargo é **0.1.0**; rótulos do roteiro, como v0.2–v0.4, descrevem marcos de implementação, não versões publicadas. Em 2026-10-15, o repositório não tinha tags Git nem GitHub Releases. Use o início rápido a partir do código-fonte abaixo; a instalação por um registro de pacotes não é uma forma de distribuição verificada. A [lista de verificação de publicação](docs/RELEASING.md) registra um impedimento atual: 18 crates no workspace, mas apenas 17 entradas na ordem de publicação (`vyane-quota` está ausente).

| Disponível no código público | Limitação |
| --- | --- |
| Envio para um destino, distribuição para vários, failover explícito, streaming e histórico local | Um destino HTTP direto fornece respostas do modelo, não ferramentas de sistema de arquivos ou shell. |
| Roteamento determinístico e fluxos de trabalho declarativos | O roteamento depende dos seus perfis; não garante automaticamente a qualidade. |
| CLI, API REST na interface de loopback e MCP por stdio | As interfaces expõem subconjuntos diferentes; consulte o [guia da CLI](docs/CLI-GUIDE.md). |
| Tarefas persistentes, mensagens e objetivos locais, e execuções residentes Process/Native no Linux | As execuções Native começam do zero e não têm sessão; a retomada após aprovação, a retomada de sessões nativas e a reexecução automática após reinício ainda estão pendentes. |
| Permissões explícitas, ambientes de processos filhos sanitizados e contabilização de execuções | A aplicação do sandbox da CLI é delegada ao ambiente de execução selecionado. Os campos de proprietário e a autenticação de loopback não oferecem isolamento contra processos hostis do mesmo usuário nem isolamento entre usuários. |

Linux e macOS são as plataformas de CI; alguns recursos de execução residente e sandbox são específicos do Linux. Não se afirma que há suporte ao Windows. O acesso aos modelos, as credenciais e qualquer CLI de agente de programação devem ser fornecidos separadamente. As chamadas a provedores podem gerar custos; a compilação e as verificações offline abaixo não chamam modelos.

## Início rápido

Instale o Rust estável com o Cargo e as ferramentas de compilação C/C++ da sua plataforma. A versão mínima declarada do Rust é 1.88. O repositório seleciona o canal estável com rustfmt e Clippy. Execute a partir da raiz do repositório:

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
"$VYANE" --help
```

A versão deve exibir `vyane 0.1.0`. A instalação ocorre dentro desta cópia do repositório, portanto não substitui outro `vyane` no seu PATH. Mantenha este shell aberto para os próximos comandos. Para reproduzir este estado da documentação, selecione a revisão `b46c194` antes de instalar; `main` pode mudar.

### Primeira execução sem credenciais

```sh
cat > ./target/offline-config.toml <<'TOML'
[providers.offline]
base_url = "https://example.invalid/v1"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "offline-placeholder"

[profiles.offline]
provider = "offline"
harness = "none"
model = "offline-placeholder"
TOML
"$VYANE" --config ./target/offline-config.toml check
"$VYANE" --config ./target/offline-config.toml route "Explain this function" --json
```

`check` deve informar que a configuração é válida e inclui o perfil `offline`. `route` retorna JSON com `"selection_key": "offline"` e `"model": "offline-placeholder"` sem enviar uma tarefa. Este é um exemplo de configuração offline, não uma resposta simulada de modelo nem um provedor funcional; não envie tarefas ao endpoint deliberadamente inválido.

### Configurar um destino real

Crie `./target/model-config.toml` com a estrutura abaixo. Substitua o endpoint e o modelo de exemplo por valores aceitos pelo seu provedor; `example.invalid` foi escolhido justamente por não ser um serviço funcional.

```toml
[providers.demo]
base_url = "https://example.invalid/v1"
api_key_env = "VYANE_DEMO_API_KEY"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "YOUR_PROVIDER_MODEL_ID"

[profiles.chat]
provider = "demo"
protocol = "openai_chat"
harness = "none"
model = "YOUR_PROVIDER_MODEL_ID"
```

Defina `VYANE_DEMO_API_KEY` pelo seu gerenciador de segredos habitual ou pelo ambiente do shell; nunca coloque o valor no TOML nem o inclua em um commit. Em seguida:

```sh
"$VYANE" --config ./target/model-config.toml check
"$VYANE" --config ./target/model-config.toml dispatch "Explain a Rust trait in one paragraph" --target chat
```

O primeiro comando verifica a configuração e a presença das credenciais; não comprova que o endpoint aceita suas credenciais ou o modelo. O segundo envia uma solicitação real depois que os valores de exemplo forem substituídos. O sucesso de uma chamada a um provedor real não faz parte da verificação do início rápido offline. Para ambientes de execução de agentes de programação, permissões, múltiplos perfis e failover, consulte os [exemplos de configuração](profiles.example.toml) e o [guia da CLI](docs/CLI-GUIDE.md).

## Como os destinos funcionam

![CLI, REST e MCP chamam o serviço e o núcleo; cada destino associa provedor, protocolo, ambiente de execução e modelo; as execuções produzem registros.](docs/assets/target-flow.svg)

| Campo | Significado |
| --- | --- |
| `provider` | Endpoint, origem das credenciais e identidade de faturamento. |
| `protocol` | Formato de comunicação: OpenAI Chat, OpenAI Responses ou Anthropic Messages. |
| `harness` | Adaptador de execução, como Claude Code ou Codex CLI; `none` significa HTTP direto. |
| `model` | Identificador de modelo dentro do namespace desse provedor. |

Um perfil associa esses quatro campos. Cada destino de failover é resolvido separadamente, portanto um identificador de modelo não é reutilizado silenciosamente com outro provedor. O núcleo depende de traits compartilhados; o serviço reúne os adaptadores concretos. Consulte a [arquitetura](docs/ARCHITECTURE.md) para detalhes sobre dependências e limites de autoridade.

## Exemplos e referência

Depois de configurar os perfis indicados, estes comandos ilustram os próximos passos:

```sh
"$VYANE" --config ./target/model-config.toml dispatch "Explain this algorithm" --target chat --stream
"$VYANE" --config ./target/model-config.toml broadcast "Compare two implementation approaches" --targets chat,review
"$VYANE" --config ./target/model-config.toml route "Review this design" --json
```

O exemplo de broadcast exige um segundo perfil chamado `review`. Os prompts são texto literal: mencionar um arquivo não anexa seu conteúdo a uma solicitação HTTP direta. O [guia da CLI](docs/CLI-GUIDE.md) aborda fluxos de trabalho, ciclo de vida do daemon, autenticação REST, MCP, mensagens locais e objetivos. Use `--help` em cada subcomando para consultar os comandos e as opções atuais.

## Relação com o Python Vyane

Este repositório é a implementação pública em Rust de capacidades do sistema privado original Python Vyane. Não é uma substituição direta: comandos Python, configuração, sessões armazenadas, fluxos de trabalho e integrações operacionais devem ser verificados individualmente antes da migração. Não aponte as duas implementações para o mesmo diretório de estado sem um caminho de migração documentado.

A [matriz pública de capacidades](docs/parity/ORIGINAL-VYANE-PARITY.md) acompanha 53 capacidades em oito áreas: 7 implementadas, 23 parciais, 12 ausentes, 9 deliberadamente diferentes ou aguardando uma decisão e 2 planejadas. O [manifesto sanitizado](docs/parity/fixtures/v1/manifest.json) contém 44 casos em 5 suítes: 30 correspondências normalizadas e 14 diferenças em aberto. São verificações delimitadas de comportamento, não um percentual de paridade do sistema inteiro. Recalcule as evidências públicas da implementação em Rust com:

```sh
python3 .github/scripts/parity-report.py --format markdown
```

Isso executa as verificações de fixtures em Rust e valida a atestação sanitizada armazenada; não executa a implementação privada de referência. Colaboradores públicos não precisam de acesso ao código privado. Consulte o [roteiro](docs/ROADMAP.md) para o trabalho pendente e o [registro de alterações](docs/CHANGELOG.md) para as mudanças no repositório público.

## Contribuir e obter ajuda

Comece por [CONTRIBUTING.md](CONTRIBUTING.md) para conhecer as verificações e as regras de revisão. Contribuições úteis incluem relatos de bugs reproduzíveis, exemplos menores, correções de tradução e testes para lacunas de capacidade documentadas. Pesquise nas [Issues](https://github.com/zleo-ai/vyane-rs/issues) e nas [PRs abertas](https://github.com/zleo-ai/vyane-rs/pulls) antes de iniciar um trabalho que possa se sobrepor a outro. Inclua a revisão do código-fonte, o sistema operacional, a versão do Rust, uma reprodução mínima e a saída sanitizada. Nunca inclua chaves, prompts privados ou caminhos pessoais do sistema de arquivos.

Para vulnerabilidades, siga [SECURITY.md](SECURITY.md) antes de publicar detalhes. A cobertura e o estado de revisão das traduções estão em [TRANSLATING.md](docs/TRANSLATING.md); a manutenção de versões e publicações está em [RELEASING.md](docs/RELEASING.md).

## Licença

Licença dupla [MIT](LICENSE-MIT) ou [Apache-2.0](LICENSE-APACHE), à sua escolha. As contribuições seguem os mesmos termos; consulte o [guia de contribuição](CONTRIBUTING.md#licensing).
