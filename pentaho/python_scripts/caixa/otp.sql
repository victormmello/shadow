SELECT
YEAR(cp.entrega)*100 + MONTH(cp.entrega) as data_entrega,
CONVERT(NVARCHAR,cp.entrega + COALESCE(pp.vencimento,60),103) as data_parcela,
LTRIM(RTRIM(c.fornecedor)) as fornecedor,
SUM(cp.valor_entregar * COALESCE(pp.percentual,1)) as custo
FROM compras_produto cp
INNER JOIN compras c on cp.pedido = c.pedido
INNER JOIN produtos p on p.produto = cp.produto
LEFT JOIN bi_prazos_fornecedor pf on pf.fornecedor = c.fornecedor
LEFT JOIN bi_prazos_pagamento pp on pp.prazo = pf.prazo
WHERE
p.grupo_produto != 'GIFTCARD' and
YEAR(cp.entrega)*100 + MONTH(cp.entrega) >= YEAR(GETDATE()-1)*100 + MONTH(GETDATE()-1) and
cp.entrega < GETDATE()+180 and
COALESCE(cp.valor_entregue,0) = 0
group by
YEAR(cp.entrega)*100 + MONTH(cp.entrega),
CONVERT(NVARCHAR,cp.entrega + COALESCE(pp.vencimento,60),103),
LTRIM(RTRIM(c.fornecedor))