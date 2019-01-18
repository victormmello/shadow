IF OBJECT_ID('bi_transferencia_melhoria_availability') IS NOT NULL
  DROP TABLE bi_transferencia_melhoria_availability;
SELECT 
* 
INTO dbo.bi_transferencia_melhoria_availability
FROM (
	SELECT
	dpb.produto,
	dpb.cor_produto,
	MAX(p.desc_produto) as descricao,
	MAX(pc.desc_cor_produto) as cor,
	dpb.filial as filial_receber,
	SUM(dpb.disponibilidade_filial) as disponibilidade_atual,
	MAX(doar.filial) as filial_doar,
	SUM(doar.quantidade_estoque_filial) as quantidade_doar,
	SUM(CASE WHEN dpb.disponibilidade_filial = 0 THEN doar.disponibilidade_filial ELSE 0 END) as delta_disponibilidade,
	ROW_NUMBER() OVER (
		PARTITION BY dpb.produto, dpb.cor_produto
		ORDER BY
			SUM(CASE WHEN dpb.disponibilidade_filial = 0 THEN doar.disponibilidade_filial ELSE 0 END) desc,
			SUM(CASE WHEN dpb.disponibilidade_filial = 0 THEN doar.disponibilidade_filial ELSE 0 END) + SUM(dpb.disponibilidade_filial) desc,
			dpb.filial
	) as linha
	FROM bi_disponibilidade_produto_barra dpb
	INNER JOIN produtos p on p.produto = dpb.produto
	INNER JOIN produto_cores pc on
		pc.produto = dpb.produto and
		pc.cor_produto = dpb.cor_produto
	INNER JOIN (
		SELECT
		dpc.produto,
		dpc.cor_produto,
		MAX(dpc.filial) as filial
		FROM bi_disponibilidade_produto_cor dpc
		INNER JOIN (
			SELECT
			dpc.produto,
			dpc.cor_produto,
			MIN(dpc.disponibilidade_filial) as min_disponibilidade_filial
			FROM bi_disponibilidade_produto_cor dpc
			WHERE
			dpc.disponibilidade_filial > 0 and
			dpc.disponibilidade_filial < 0.3
			GROUP BY
			dpc.produto,
			dpc.cor_produto
		) m on
			m.produto = dpc.produto and
			m.cor_produto = dpc.cor_produto and
			m.min_disponibilidade_filial = dpc.disponibilidade_filial
		GROUP BY
		dpc.produto,
		dpc.cor_produto
	) min_ava on
		min_ava.produto = dpb.produto and
		min_ava.cor_produto = dpb.cor_produto
	INNER JOIN bi_disponibilidade_produto_barra doar on
		doar.filial = min_ava.filial and
		doar.codigo_barra = dpb.codigo_barra
	WHERE
	dpb.filial != min_ava.filial and
	(dpb.filial != 'e-commerce' or dpb.produto IN (select produto from bi_disponivel_vtex where ativo = 1))
	GROUP BY
	dpb.filial,
	dpb.produto,
	dpb.cor_produto
	HAVING
	SUM(dpb.disponibilidade_filial) > 0.2 and
	SUM(CASE WHEN dpb.disponibilidade_filial = 0 THEN doar.disponibilidade_filial ELSE 0 END) > 0
) t
WHERE t.linha = 1