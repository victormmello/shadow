SELECT
cs.anomes, -- tem que estar em primeiro
-- dimensoes:
%(dimensions)s,
-- metricas:
%(metrics)s,
-- ultima venda:
MAX(cs.ultima_venda) as ultima_venda -- tem que estar em ultimo
FROM bi_category_sales cs
WHERE
cs.anomes BETWEEN YEAR(DATEADD(mm,-19,GETDATE()-1))*100 + MONTH(DATEADD(mm,-19,GETDATE()-1)) AND YEAR(GETDATE()-1)*100 + MONTH(GETDATE()-1)
GROUP BY
cs.anomes,
%(dimensions)s
ORDER BY
cs.anomes,
%(dimensions)s