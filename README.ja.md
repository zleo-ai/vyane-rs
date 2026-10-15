# Vyane

**1 つの Rust CLI からコーディングエージェントとモデル API を実行。**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

複数のコーディングエージェントやモデル提供元を使う開発者向けです。実行先に名前を付け、1 つまたは複数の実行先にタスクを送り、結果と試行履歴をまとめて残せます。コーディングには Claude Code や Codex CLI を使い、テキスト応答にはモデル API を直接呼び出せます。

> **開発初期段階です。** 現在はソースからのインストールのみで、API は不安定です。未完成・未実装の機能があります。ネイティブセッションの再開と再起動後の自動再実行は未完成です。[現在の制約](docs/PROJECT-STATUS.md)を確認してください。

## 動作を見る

[ターミナルデモを実行](docs/EXAMPLES.md)：インストール済み CLI が単一タスクを実行し、2 つの実行先へ同時送信した後、意図的に発生させた主実行先の障害から復旧します。ローカル HTTP テスト用応答を使うため、キーは不要で、実際のモデルにはアクセスしません。

```text
Local HTTP fixtures only; no live model calls.
dispatch: fixture: chat-model; recorded attempts=1
broadcast: 2 separate successful results (chat-model, review-model)
failover: fixture: review-model; recorded attempts=2
```

## 3 つの使い方

| やりたいこと | コマンド | 得られるもの |
| --- | --- | --- |
| 選んだエージェントやモデルでタスクを実行する | [`dispatch --target chat`](docs/EXAMPLES.md#one-task-one-target) | 1 つの結果とローカル実行記録。コーディング用ハーネスは別途インストール・設定が必要です。 |
| 同じプロンプトへの回答を比較する | [`broadcast --targets chat,review`](docs/EXAMPLES.md#one-prompt-several-targets) | 並行実行した個別の結果。回答の評価は利用者が行います。 |
| 呼び出し失敗時に設定済みの代替先を試す | [`dispatch --target resilient`](docs/EXAMPLES.md#an-explicit-backup) | 対象となるエラーに対する順序付きフェイルオーバーと各試行の記録。セッションの引き継ぎではありません。 |

## クイックスタート

**1. ソースからインストール。** stable Rust/Cargo（Rust 1.88 以上）と C/C++ ビルドツールが必要です。CI 対象は Linux と macOS です。Windows 対応は表明していません。

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
```

期待される表示は `vyane 0.1.0` です。バイナリはこのチェックアウト内に置かれ、PATH 上の既存の `vyane` は置き換えません。このシェルを開いたままにしてください。

**2. キーなしで最初の結果を得る。** 同梱の例はルーティングのプレビューのみで、モデルへのリクエストを送りません。

```sh
"$VYANE" --config docs/examples/offline.toml check
"$VYANE" --config docs/examples/offline.toml route "Explain this function" --json
```

`offline` プロファイルと JSON 内の `"selection_key": "offline"` を確認してください。エンドポイントは意図的に無効にしてあるため、タスクを送信しないでください。

**3. 自分の実行先を接続。** [プロバイダー設定](docs/QUICKSTART.md#configure-a-real-target)に従い、[3 つの例](docs/EXAMPLES.md)を試してください。モデルへのアクセスと認証情報は各自で用意します。実際の呼び出しには提供元の料金が発生する場合があります。オフラインのプレビューでは実サービスの動作を検証しません。

## 現在使える機能

| ソースで利用できる機能 | 制約 |
| --- | --- |
| 単一実行、同時送信、明示的フェイルオーバー、ストリーミング | 直接 HTTP はモデルのテキストを返します。ローカルファイルの添付やシェルツールは提供しません。 |
| Claude Code と Codex CLI のアダプター | CLI 子プロセス経由であり、Codex App Server RPC や Claude Agent SDK の直接統合ではありません。CLI とアクセス手段は各自で用意し、サンドボックスの適用はハーネスに委ねます。 |
| プロファイルに基づくルーティングとローカル実行履歴 | 設定に従うルーティングであり、回答品質は保証しません。 |
| CLI、REST、MCP の入口 | 利用できる操作は入口ごとに異なります。[CLI ガイド](docs/CLI-GUIDE.md)を参照してください。 |

常駐実行、永続ワークフロー、ローカル目標管理は発展途上です。ネイティブ実行は毎回新規でセッションを持ちません。ネイティブの承認・セッション再開と再起動後の自動再実行は未対応です。owner フィールドとループバック認証は、悪意ある同一ユーザーや複数ユーザー間の分離を提供しません。これらを利用する前に[状況と制限](docs/PROJECT-STATUS.md)を確認してください。

## 詳しく知る

- [アーキテクチャと実行先モデル](docs/ARCHITECTURE.md) · [設定リファレンス](profiles.example.toml) · [CLI・REST・MCP ガイド](docs/CLI-GUIDE.md)
- [機能一覧と再現可能な証拠](docs/PROJECT-STATUS.md#relationship-to-python-vyane) · [ロードマップ](docs/ROADMAP.md)
- [変更履歴](docs/CHANGELOG.md) · [バージョンとリリース方針](docs/RELEASING.md)

非公開の元の Python Vyane システムの機能を Rust で公開実装したものです。そのまま置き換えられるものではなく、コマンド、設定、保存済みセッション、ワークフローを個別に移行確認する必要があります。文書化された移行手順なしで状態ディレクトリを共有しないでください。公開プロジェクトへの貢献に非公開ソースへのアクセスは不要です。

## 貢献と問い合わせ

例を試す、再現可能な不具合を報告する、翻訳を改善する、既知の機能不足を補う、といった貢献を歓迎します。[Issues](https://github.com/zleo-ai/vyane-rs/issues) と[進行中の PR](https://github.com/zleo-ai/vyane-rs/pulls)を確認し、[CONTRIBUTING.md](CONTRIBUTING.md)を読んでください。ソースのリビジョンと機密情報を除いた出力を添え、キー、非公開プロンプト、個人のパスは含めないでください。脆弱性は [SECURITY.md](SECURITY.md) に従って報告してください。

## ライセンス

[MIT](LICENSE-MIT) または [Apache-2.0](LICENSE-APACHE) を選択できます。貢献にも同じ条件が適用されます。

---

ソース基準：`fe43941` · ワークスペース：`0.1.0` · 更新：2026-10-15。英語版が正本です。本訳は機械支援による草稿で、母語話者による確認待ちです。[翻訳範囲と確認状況](docs/TRANSLATING.md)を参照してください。ロードマップの節目は公開済みリリースを意味しません。
