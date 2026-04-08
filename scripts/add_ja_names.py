#!/usr/bin/env python3
"""stock_meta.jsonに日本語企業名を追加（主要銘柄のみ手動、残りは英語名短縮）"""
import json, re
from pathlib import Path

META = Path(__file__).resolve().parent.parent / "public" / "api" / "stock_meta.json"

JA = {
"AAPL":"アップル","ABBV":"アッヴィ","ABNB":"エアビーアンドビー","ABT":"アボット","ACN":"アクセンチュア",
"ADBE":"アドビ","ADI":"アナログ・デバイセズ","ADP":"ADP","ADSK":"オートデスク",
"AFRM":"アファーム","AI":"C3.ai","ALAB":"アサテラ","ALB":"アルベマール",
"AMAT":"アプライドマテリアルズ","AMD":"AMD","AMGN":"アムジェン","AMT":"アメリカンタワー",
"AMZN":"アマゾン","ANET":"アリスタ","ANSS":"アンシス","APD":"エアープロダクツ",
"APH":"アンフェノール","APTV":"アプティブ","ARKG":"ARKゲノミクスETF",
"ARKK":"ARKイノベーションETF","ASML":"ASML","AVGO":"ブロードコム","AXP":"アメックス",
"BA":"ボーイング","BABA":"アリババ","BAC":"バンクオブアメリカ","BAX":"バクスター",
"BHP":"BHPグループ","BIIB":"バイオジェン","BK":"バンクオブNY","BKNG":"ブッキング",
"BKR":"ベーカーヒューズ","BLK":"ブラックロック","BMY":"ブリストルマイヤーズ",
"BRK.B":"バークシャー","BSX":"ボストンサイエンティフィック","BX":"ブラックストーン",
"C":"シティグループ","CAT":"キャタピラー","CCL":"カーニバル","CDNS":"ケイデンス",
"CEG":"コンステレーション","CF":"CFインダストリーズ","CHTR":"チャーター",
"CI":"シグナ","CL":"コルゲート","CLSK":"クリーンスパーク","CMCSA":"コムキャスト",
"CME":"CMEグループ","CMG":"チポトレ","CMS":"CMSエナジー","CNP":"センターポイント",
"COF":"キャピタルワン","COIN":"コインベース","COP":"コノコフィリップス",
"COST":"コストコ","CRM":"セールスフォース","CRSP":"クリスパー","CRWD":"クラウドストライク",
"CSCO":"シスコ","CTAS":"シンタス","CVS":"CVSヘルス","CVX":"シェブロン",
"D":"ドミニオン","DDOG":"データドッグ","DE":"ディア","DG":"ダラーゼネラル",
"DHR":"ダナハー","DIS":"ディズニー","DLTR":"ダラーツリー","DOCU":"ドキュサイン",
"DOW":"ダウ","DUK":"デューク","DVN":"デボン","DXCM":"デクスコム",
"EA":"エレクトロニック・アーツ","ECL":"エコラボ","EDR":"エンデバー",
"EL":"エスティローダー","EMR":"エマソン","ENPH":"エンフェーズ",
"EOG":"EOGリソーシズ","EQIX":"エクイニクス","ETN":"イートン","EW":"エドワーズ",
"EXPE":"エクスペディア","F":"フォード","FANG":"ダイヤモンドバック","FAST":"ファスナル",
"FCX":"フリーポート","FDX":"フェデックス","FI":"フィサーブ","FISV":"フィサーブ",
"FTNT":"フォーティネット","GD":"ゼネラルダイナミクス","GE":"GEエアロスペース",
"GILD":"ギリアド","GIS":"ゼネラルミルズ","GLW":"コーニング","GM":"GM",
"GOOG":"アルファベット","GOOGL":"アルファベット","GPN":"グローバルペイメンツ",
"GS":"ゴールドマン","HAL":"ハリバートン","HCA":"HCAヘルスケア","HD":"ホームデポ",
"HIMS":"ヒムズ","HON":"ハネウェル","HPQ":"HP","HUM":"ヒューマナ",
"IBIT":"iSharesビットコインETF","IBM":"IBM","ICE":"ICE","IDXX":"アイデックス",
"INTC":"インテル","INTU":"インテュイット","IOT":"サムサラ","IONQ":"IonQ",
"ISRG":"インテュイティブ","ITW":"イリノイツールワークス",
"JCI":"ジョンソンコントロールズ","JNJ":"ジョンソン&ジョンソン","JPM":"JPモルガン",
"KHC":"クラフトハインツ","KLAC":"KLA","KMB":"キンバリークラーク","KO":"コカコーラ",
"KR":"クローガー","KVUE":"キュービュー","LBRDK":"リバティブロードバンド",
"LBRDA":"リバティブロードバンドA","LEN":"レナー","LIN":"リンデ","LLY":"イーライリリー",
"LMT":"ロッキード","LNTH":"ランセウス","LOW":"ロウズ","LRCX":"ラムリサーチ",
"LULU":"ルルレモン","LUV":"サウスウエスト","LVS":"ラスベガスサンズ",
"LYV":"ライブネーション","MA":"マスターカード","MAR":"マリオット",
"MARA":"マラソンデジタル","MCD":"マクドナルド","MCHP":"マイクロチップ",
"MCK":"マッケソン","MCO":"ムーディーズ","MDLZ":"モンデリーズ","MDT":"メドトロニック",
"MELI":"メルカドリブレ","MET":"メットライフ","META":"メタ",
"MGM":"MGMリゾーツ","MKC":"マコーミック","MMM":"スリーエム","MNST":"モンスター",
"MO":"アルトリア","MOH":"モリーナ","MOS":"モザイク","MPC":"マラソン石油",
"MRNA":"モデルナ","MRK":"メルク","MRVL":"マーベル","MS":"モルガンスタンレー",
"MSCI":"MSCI","MSFT":"マイクロソフト","MSI":"モトローラ","MU":"マイクロン",
"NCLH":"ノルウェージャンクルーズ","NDAQ":"ナスダック","NEE":"ネクステラ",
"NET":"クラウドフレア","NFLX":"ネットフリックス","NKE":"ナイキ","NOC":"ノースロップ",
"NOW":"サービスナウ","NSC":"ノーフォーク","NTDOY":"任天堂","NUE":"ニューコア",
"NVDA":"エヌビディア","NVO":"ノボノルディスク","NXPI":"NXP",
"ODFL":"オールドドミニオン","OKE":"ONEOK","ON":"オンセミ","ORCL":"オラクル",
"OXY":"オキシデンタル","PANW":"パロアルト","PARA":"パラマウント","PATH":"UiPath",
"PAYC":"ペイコム","PAYX":"ペイチェックス","PDD":"PDD","PEP":"ペプシコ",
"PFE":"ファイザー","PG":"P&G","PGR":"プログレッシブ","PLD":"プロロジス",
"PLTR":"パランティア","PM":"フィリップモリス","PNC":"PNC","POOL":"プール",
"PSA":"パブリックストレージ","PSX":"フィリップス66","PYPL":"ペイパル",
"QCOM":"クアルコム","QQQ":"NASDAQ100ETF","RBLX":"ロブロックス",
"REGN":"リジェネロン","RIVN":"リビアン","RMD":"レスメド","ROKU":"ロク",
"ROP":"ローパー","ROST":"ロスストアーズ","RSP":"S&P500均等ETF","RTX":"RTX",
"SBUX":"スターバックス","SCHW":"チャールズシュワブ","SE":"シー","SHOP":"ショッピファイ",
"SHW":"シャーウィンウィリアムズ","SLB":"シュルンベルジェ","SMCI":"スーパーマイクロ",
"SNAP":"スナップ","SNPS":"シノプシス","SO":"サザン","SPG":"サイモンプロパティ",
"SPOT":"スポティファイ","SPY":"S&P500ETF","SQ":"ブロック","SRE":"センプラ",
"SSNC":"SSテクノロジーズ","STZ":"コンステレーションブランズ","SYK":"ストライカー",
"T":"AT&T","TDG":"トランスダイム","TGT":"ターゲット","TJX":"TJX","TMO":"サーモフィッシャー",
"TMUS":"Tモバイル","TSLA":"テスラ","TSM":"TSMC","TSN":"タイソン",
"TT":"トレインテクノロジーズ","TTD":"トレードデスク","TTWO":"テイクツー",
"TXN":"テキサスインスツルメンツ","U":"ユニティ","UBER":"ウーバー","UNH":"ユナイテッドヘルス",
"UNP":"ユニオンパシフィック","UPS":"UPS","URI":"ユナイテッドレンタルズ",
"USB":"USバンコープ","V":"ビザ","VEEV":"ヴィーバ","VICI":"ヴィチ",
"VLO":"バレロ","VRSK":"ベリスク","VRTX":"バーテックス","VZ":"ベライゾン",
"W":"ウェイフェア","WBA":"ウォルグリーンズ","WBD":"ワーナーブラザーズ",
"WDAY":"ワークデイ","WEC":"WECエナジー","WELL":"ウェルタワー","WFC":"ウェルズファーゴ",
"WM":"ウェイストマネジメント","WMT":"ウォルマート","WST":"ウエスト",
"XOM":"エクソンモービル","XYL":"ザイレム","YUM":"ヤムブランズ",
"ZBH":"ジンマーバイオメット","ZM":"ズーム","ZS":"ゼットスケーラー","ZTS":"ゾエティス",
}

def shorten(name):
    """英語名を短縮（Inc., Corp., Ltd.等を除去）"""
    for suf in [", Inc.","Inc.","Corporation","Corp.","Ltd.","Limited","LLC","PLC","plc",
                "Holdings","Group","Technologies","Technology","International","Incorporated",
                "Company","Co.","& Co","N.V.","SE","S.A.","S.p.A.",", L.P."]:
        name = name.replace(suf, "")
    return name.strip().rstrip(",").strip()

with open(META, encoding="utf-8") as f:
    meta = json.load(f)

for tk, data in meta.items():
    if tk in JA:
        data["name_ja"] = JA[tk]
    else:
        data["name_ja"] = shorten(data.get("name", tk))

with open(META, "w", encoding="utf-8") as f:
    json.dump(meta, f, ensure_ascii=False, indent=1)

print(f"Updated {len(meta)} entries. JA mapped: {sum(1 for tk in meta if tk in JA)}")
