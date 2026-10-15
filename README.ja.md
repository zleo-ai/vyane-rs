# Vyane

**1 つの Rust CLI とオーケストレーションカーネルで、AI モデルのタスクを実行・連携します。**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

ソース基準：`b46c194` · ワークスペースのバージョン：`0.1.0` · 更新日：2026-10-15。
英語版を正本とします。この翻訳は機械支援による草稿であり、ネイティブ話者による校閲済みとはしていません。保守ルールは[翻訳ポリシー](docs/TRANSLATING.md)を参照してください。

Vyane は、設定した 1 つのターゲットへのタスクのディスパッチ、複数ターゲットへのブロードキャスト、または明示したフェイルオーバーチェーンの順次試行を行います。ターゲットは Claude Code や Codex CLI の実行アダプターを利用するほか、HTTP モデルエンドポイントを直接呼び出すこともできます。ローカルでエージェントのワークフローを構築する開発者や、明示的なルーティング、実行権限、キャンセル、実行記録を必要とする Rust アプリケーションの開発者向けです。

![抽象的なターミナルウィンドウが銅色の調整ハブを通じて結果カードにつながっている。](docs/assets/vyane-hero.webp)

*AI で生成したコンセプトイラストであり、アーキテクチャ図ではありません。[アセットについて](docs/assets/README.md)。*

## 現状と制約

本プロジェクトは初期段階にあり、ソースからのビルドが必要で、API はまだ安定していません。Cargo のバージョンは **0.1.0** です。ロードマップの v0.2–v0.4 などのラベルは実装上のマイルストーンを示すもので、公開済みのリリースではありません。2026-10-15 時点で、このリポジトリには Git タグも GitHub Releases もありませんでした。以下のソースビルドによるクイックスタートを利用してください。レジストリからのインストールは、確認済みの配布方法ではありません。[リリースチェックリスト](docs/RELEASING.md)には現在の阻害要因として、ワークスペースの crate が 18 個あるのに対し、公開順序の一覧には 17 個しかなく、`vyane-quota` が欠けていることを記録しています。

| 公開ソースで利用できる機能 | 制約 |
| --- | --- |
| ディスパッチ、ブロードキャスト、明示的なフェイルオーバー、ストリーミング、ローカル履歴 | 直接 HTTP ターゲットが提供するのはモデルの応答であり、ファイルシステムやシェルのツールではありません。 |
| 決定的なルーティングと宣言的ワークフロー | ルーティングは設定したプロファイルに依存し、品質を自動的に保証するものではありません。 |
| CLI、ループバック REST API、標準入出力による MCP | 各フロントエンドで公開する機能の範囲は異なります。[CLI ガイド](docs/CLI-GUIDE.md)を参照してください。 |
| 永続化タスク、ローカルのメッセージとゴール、Linux 常駐型 Process/Native 実行 | Native 実行は新規かつセッションなしです。承認後の再開、ネイティブセッションの再開、再起動後の自動リプレイは未実装です。 |
| 明示的な権限、クリーンアップした子プロセス環境、実行の使用量記録 | CLI のサンドボックス制御は選択した実行アダプターに委ねられます。owner フィールドとループバック認証は、同一 OS ユーザーの悪意あるプロセスに対する隔離や、マルチユーザーの隔離を提供しません。 |

CI の対象は Linux と macOS です。一部の常駐実行機能とサンドボックス機能は Linux 固有です。Windows 対応は表明していません。モデルへのアクセス、認証情報、コーディングエージェントの CLI は別途用意する必要があります。プロバイダーの呼び出しには料金が発生する場合があります。ビルドと以下のオフラインチェックはモデルを呼び出しません。

## クイックスタート

Cargo を含む安定版 Rust と、プラットフォーム用の C/C++ ビルドツールチェーンをインストールしてください。宣言されている最低 Rust バージョンは 1.88 です。このリポジトリでは rustfmt と Clippy を含む stable ツールチェーンを選択しています。リポジトリのルートで実行してください。

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
"$VYANE" --help
```

バージョン出力は `vyane 0.1.0` になるはずです。このチェックアウト内にインストールするため、PATH 上にある別の `vyane` を置き換えることはありません。続くコマンドを実行するため、このシェルを開いたままにしてください。このドキュメントのスナップショットを再現するには、インストール前に `b46c194` をチェックアウトしてください。`main` は変更される可能性があります。

### 認証情報なしで初めて実行する

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

`check` は、`offline` プロファイルを含む有効な設定であることを報告するはずです。`route` はディスパッチを行わず、`"selection_key": "offline"` と `"model": "offline-placeholder"` を含む JSON を返します。これはオフライン設定の例であり、模擬的なモデル応答でも、利用可能なプロバイダーでもありません。意図的に無効にしているエンドポイントへタスクをディスパッチしないでください。

### 実際のターゲットを設定する

次の構造で `./target/model-config.toml` を作成してください。例のエンドポイントとモデルを、利用するプロバイダーがサポートする値に置き換えます。`example.invalid` は意図的に動作しない例示用のドメインです。

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

通常利用しているシークレット管理ツールやシェル環境で `VYANE_DEMO_API_KEY` を設定してください。値を TOML に書き込んだり、コミットしたりしないでください。続いて実行します。

```sh
"$VYANE" --config ./target/model-config.toml check
"$VYANE" --config ./target/model-config.toml dispatch "Explain a Rust trait in one paragraph" --target chat
```

最初のコマンドは設定と認証情報の存在を確認しますが、エンドポイントがその認証情報やモデルを受け付けることまでは確認しません。プレースホルダーを置き換えると、2 番目のコマンドは実際のリクエストを送信します。実プロバイダーへの呼び出し成功は、オフラインのクイックスタート検証には含まれません。コーディング用の実行アダプター、権限、複数のプロファイル、フェイルオーバーについては、[設定例](profiles.example.toml)と [CLI ガイド](docs/CLI-GUIDE.md)を参照してください。

## ターゲットの仕組み

![CLI、REST、MCP がサービスとカーネルを呼び出す。各ターゲットは provider、protocol、harness、model を結び付け、実行によって実行記録が生成される。](docs/assets/target-flow.svg)

| フィールド | 意味 |
| --- | --- |
| `provider` | エンドポイント、認証情報の取得元、課金に使用する識別情報。 |
| `protocol` | 通信形式：OpenAI Chat、OpenAI Responses、Anthropic Messages。 |
| `harness` | Claude Code や Codex CLI などの実行アダプター。`none` は直接 HTTP を意味します。 |
| `model` | そのプロバイダーの名前空間内で使うモデル識別子。 |

プロファイルはこれら 4 つのフィールドを結び付けます。フェイルオーバー先の各ターゲットは個別に解決されるため、モデル識別子が別のプロバイダーに対して暗黙に再利用されることはありません。カーネルは共有トレイトに依存し、サービスが具体的なアダプターを組み立てます。依存関係と権限の詳細は[アーキテクチャ](docs/ARCHITECTURE.md)を参照してください。

## 使用例とリファレンス

コマンドで指定するプロファイルを設定した後は、次の例を試せます。

```sh
"$VYANE" --config ./target/model-config.toml dispatch "Explain this algorithm" --target chat --stream
"$VYANE" --config ./target/model-config.toml broadcast "Compare two implementation approaches" --targets chat,review
"$VYANE" --config ./target/model-config.toml route "Review this design" --json
```

ブロードキャストの例には、`review` という名前の 2 つ目のプロファイルが必要です。プロンプトはそのままテキストとして扱われます。ファイル名を記述しても、その内容が直接 HTTP リクエストに添付されるわけではありません。[CLI ガイド](docs/CLI-GUIDE.md)では、ワークフロー、デーモンのライフサイクル、REST 認証、MCP、ローカルメッセージ、ゴールを説明しています。各サブコマンドの現在のインターフェースは `--help` で確認してください。

## Python Vyane との関係

本リポジトリは、元の非公開 Python Vyane システムの機能を Rust で実装した公開版です。そのまま置き換えられるものではありません。移行前に、Python のコマンド、設定、保存済みセッション、ワークフロー、運用上の連携を個別に確認する必要があります。移行手順が文書化されていない状態で、両実装に同じ状態ディレクトリを使わせないでください。

[公開の機能マトリクス](docs/parity/ORIGINAL-VYANE-PARITY.md)は、8 つの分野にわたる 53 項目を追跡しています。実装済み 7、一部実装 23、未実装 12、意図的に異なるか判断待ちのものが 9、計画中が 2 です。[機密情報を除去したマニフェスト](docs/parity/fixtures/v1/manifest.json)には、5 スイートにわたる 44 ケースが含まれます。正規化後に一致するものが 30、未解決の相違が 14 です。これらは範囲を限定した動作チェックであり、システム全体の互換率を示すものではありません。公開 Rust 実装の検証結果を再計算するには、次を実行します。

```sh
python3 .github/scripts/parity-report.py --format markdown
```

このコマンドは Rust のフィクスチャチェックを実行し、保存済みの機密情報を除去した検証証跡を検証します。非公開の参照実装は実行しません。公開プロジェクトへの貢献に非公開ソースへのアクセスは必要ありません。未完了の作業は[ロードマップ](docs/ROADMAP.md)、公開リポジトリの変更は[変更履歴](docs/CHANGELOG.md)を参照してください。

## 貢献とサポート

まず [CONTRIBUTING.md](CONTRIBUTING.md) でチェック項目とレビューのルールを確認してください。再現可能な不具合報告、より小さなサンプル、翻訳の修正、文書化された機能不足に対するテストなどが役立ちます。重複する可能性のある作業を始める前に、[Issues](https://github.com/zleo-ai/vyane-rs/issues) と[オープンな PR](https://github.com/zleo-ai/vyane-rs/pulls) を検索してください。報告にはソースのリビジョン、OS、Rust のバージョン、最小限の再現手順、機密情報を除去した出力を添えてください。キー、非公開のプロンプト、個人のファイルシステムパスを含めないでください。

脆弱性については、詳細を公開する前に [SECURITY.md](SECURITY.md) に従ってください。翻訳の対応範囲と校閲状況は [TRANSLATING.md](docs/TRANSLATING.md)、リリースとバージョンの保守については [RELEASING.md](docs/RELEASING.md) を参照してください。

## ライセンス

[MIT](LICENSE-MIT) または [Apache-2.0](LICENSE-APACHE) のデュアルライセンスで、どちらかを選択できます。貢献にも同じ条件が適用されます。[貢献ガイド](CONTRIBUTING.md#licensing)を参照してください。
