# 管理员使用记录 IP 定位

BridgeOps 只在管理员使用记录中查询已保存的下游 IP。浏览器只提交请求日志 ID；服务端从该日志读取 IP，并使用本机的 MaxMind-compatible `.mmdb` 数据库完成查询。运行时不会将 IP 发送给公共定位服务，也不会在 API 响应或错误中返回数据库文件路径。

## 配置

准备管理员自行维护的 City 和 ASN 数据库，例如 DB-IP Lite 或 GeoLite2。文件必须位于仓库外、运行服务账号可读的私有目录。推荐使用 DB-IP Lite 的 City/ASN `.mmdb` 文件，来源见 [DB-IP Lite](https://db-ip.com/db/download/ip-to-city-lite)，数据按 [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) 使用，并保留 DB-IP 署名。配置示例：

```yaml
geoIP:
  enabled: true
  cityDatabasePath: /var/lib/bridgeops/geoip/DB-IP-City-Lite-2026-07.mmdb
  asnDatabasePath: /var/lib/bridgeops/geoip/DB-IP-ASN-Lite-2026-07.mmdb
```

也可使用 `GEOIP_ENABLED`、`GEOIP_CITY_DATABASE_PATH` 和 `GEOIP_ASN_DATABASE_PATH`。更换或更新数据库后重启服务；BridgeOps 不会自动下载、上传或联网更新这些文件。DB-IP Lite 月度压缩包地址为 `https://download.db-ip.com/free/dbip-city-lite-YYYY-MM.mmdb.gz` 和 `https://download.db-ip.com/free/dbip-asn-lite-YYYY-MM.mmdb.gz`，安装前必须执行 `gzip -t`，解压后用 `geoip2.Open` 验证两个文件，再用仅服务运行账号可读的权限保存（当前生产服务为 root，因此为 `0600 root:root`）并重启服务。不要把 `.mmdb` 文件提交到仓库或通过浏览器上传。

DB-IP Lite 数据包含 DB-IP 的署名要求，更新部署时必须继续保留 [DB-IP Lite](https://db-ip.com/db/download/ip-to-city-lite) 归属和 CC BY 4.0 许可。

ASN 数据库应与 City 数据库同时配置。系统只有在 ASN 可识别为普通网络、城市精度半径不大于 75 km 时才显示城市、邮编和坐标。CDN、代理或云网络地址始终显示为网络出口；没有 ASN、精度半径缺失或半径过大时只显示国家/地区。所有定位只描述网络出口，不能证明实际用户所在地。

## 可信代理

下游 IP 使用 Gin 的 `server.trustedProxies` 链解析。只有 TCP 对端属于该列表时，转发头才会生效；直接暴露服务时应设为 `[]`，并且不要把整个公网网段加入可信列表。使用记录会保存“直连 IP”或“可信代理转发”来源，历史记录没有该字段时显示“来源未记录”。
