SELECT DISTINCT 
t.desc_cor,
t.cor_produto,
CASE
	WHEN t.desc_cor like '%estamp%' THEN 'Estampado'
	WHEN t.desc_cor like '%list%' THEN 'Listrado'
	ELSE 'Liso'
END as padronagem
FROM (
	SELECT DISTINCT
	RTRIM(pc.desc_cor_produto) as desc_cor,
	pc.cor_produto
	FROM produto_cores pc
	INNER JOIN produtos p on p.produto = pc.produto
	WHERE
	(
		(p.grupo_produto like '[_]%' or p.grupo_produto IN ('CALCADOS','_CALCADOS'))
		and p.data_cadastramento >= GETDATE()-360
		and pc.desc_cor_produto not like '%cancelad%'
	) OR pc.desc_cor_produto IN ('ESTAMPADO ROSE')
	
	UNION

	SELECT
	RTRIM(cb.desc_cor) as desc_cor,
	cb.cor as cor_produto
	FROM cores_basicas cb
	WHERE
	cb.data_para_transferencia >= GETDATE()-360
	OR cb.desc_cor IN ('LISTRADO CINZA')
) t
ORDER BY t.desc_cor;