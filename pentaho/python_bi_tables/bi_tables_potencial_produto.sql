IF OBJECT_ID('potencial_venda_produto_barra_dia') IS NOT NULL
  DROP TABLE potencial_venda_produto_barra_dia;
SELECT
v.data_venda,
v.codigo_barra,
MAX(v.produto) as produto,
MAX(v.cor_produto) as cor_produto,
MAX(v.tamanho) as tamanho,
SUM(v.itens) as itens,
SUM(v.receita) as receita,
SUM(v.custo) as custo,
SUM(CASE WHEN sourcing = 'online' then 2 else 1 end * v.receita) as receita_sorting
INTO dbo.potencial_venda_produto_barra_dia
FROM (
	SELECT
	'offline' as sourcing,
	CAST(vp.data_venda as DATE) as data_venda,
	vp.codigo_barra,
	MAX(vp.produto) as produto,
	MAX(vp.cor_produto) as cor_produto,
	MAX(vp.tamanho) as tamanho,
	SUM(vp.qtde) as itens,
	SUM(vp.qtde*(COALESCE(v.valor_pago/NULLIF(v.valor_venda_bruta,0),1)*vp.preco_liquido)) as receita,
	SUM(vp.custo*vp.qtde) as custo
	FROM loja_venda_produto vp
	INNER JOIN loja_venda v on
		v.codigo_filial = vp.codigo_filial and
		v.ticket = vp.ticket and
		v.data_venda = vp.data_venda
	INNER JOIN filiais f on f.cod_filial = vp.codigo_filial
	INNER JOIN produtos p on p.produto = vp.produto
	WHERE
	p.grupo_produto != 'GIFTCARD' and
	vp.qtde > 0 and
	vp.data_venda >= GETDATE()-3*360
	GROUP BY
	CAST(vp.data_venda as DATE),
	vp.codigo_barra

	UNION

	SELECT
	'online' as sourcing,
	CAST(created_at as DATE) as data_venda,
	ean as codigo_barra,
	MAX(pb.produto) as produto,
	MAX(pb.cor_produto) as cor_produto,
	MAX(pb.tamanho) as tamanho,
	SUM(voi.quantity) as itens,
	SUM(voi.price*voi.quantity) as receita,
	SUM(c.preco1*voi.quantity) as custo
	FROM bi_vtex_order_items voi
	INNER JOIN produtos_barra pb on pb.codigo_barra = voi.ean
	INNER JOIN produtos_precos c on
		c.produto = pb.produto and
		c.codigo_tab_preco = '02'
	WHERE
	CAST(created_at as DATE) BETWEEN CAST(GETDATE()-1-30 as DATE) and CAST(GETDATE()-1 as DATE)
	GROUP BY
	CAST(created_at as DATE),
	ean

	UNION

	-- Venda E-COMMMERCE:
	SELECT
	'online' as sourcing,
	CAST(f.emissao as DATE) as data_venda,
	pb.codigo_barra,
	MAX(pb.produto) as produto,
	MAX(pb.cor_produto) as cor_produto,
	MAX(pb.tamanho) as tamanho,
	SUM(CASE pb.tamanho
		WHEN 1 THEN fp.f1
		WHEN 2 THEN fp.f2
		WHEN 3 THEN fp.f3
		WHEN 4 THEN fp.f4
		WHEN 5 THEN fp.f5
		WHEN 6 THEN fp.f6
		WHEN 7 THEN fp.f7
		WHEN 8 THEN fp.f8
		WHEN 9 THEN fp.f9
		WHEN 10 THEN fp.f10
		WHEN 11 THEN fp.f11
		WHEN 12 THEN fp.f12
		WHEN 13 THEN fp.f13
		WHEN 14 THEN fp.f14
		WHEN 15 THEN fp.f15
		WHEN 16 THEN fp.f16
		WHEN 17 THEN fp.f17
		WHEN 18 THEN fp.f18
		WHEN 19 THEN fp.f19
		WHEN 20 THEN fp.f20
		WHEN 21 THEN fp.f21
		WHEN 22 THEN fp.f22
		WHEN 23 THEN fp.f23
		WHEN 24 THEN fp.f24
		WHEN 25 THEN fp.f25
		WHEN 26 THEN fp.f26
		WHEN 27 THEN fp.f27
		WHEN 28 THEN fp.f28
		WHEN 29 THEN fp.f29
		WHEN 30 THEN fp.f30
		WHEN 31 THEN fp.f31
		WHEN 32 THEN fp.f32
		WHEN 33 THEN fp.f33
		WHEN 34 THEN fp.f34
		WHEN 35 THEN fp.f35
		WHEN 36 THEN fp.f36
		WHEN 37 THEN fp.f37
		WHEN 38 THEN fp.f38
		WHEN 39 THEN fp.f39
		WHEN 40 THEN fp.f40
		WHEN 41 THEN fp.f41
		WHEN 42 THEN fp.f42
		WHEN 43 THEN fp.f43
		WHEN 44 THEN fp.f44
		WHEN 45 THEN fp.f45
		WHEN 46 THEN fp.f46
		WHEN 47 THEN fp.f47
		WHEN 48 THEN fp.f48
		ELSE 0
	END) as itens,
	SUM(f.valor_total/f.valor_sub_itens*(fp.valor/fp.qtde*
		CASE pb.tamanho
			WHEN 1 THEN fp.f1
			WHEN 2 THEN fp.f2
			WHEN 3 THEN fp.f3
			WHEN 4 THEN fp.f4
			WHEN 5 THEN fp.f5
			WHEN 6 THEN fp.f6
			WHEN 7 THEN fp.f7
			WHEN 8 THEN fp.f8
			WHEN 9 THEN fp.f9
			WHEN 10 THEN fp.f10
			WHEN 11 THEN fp.f11
			WHEN 12 THEN fp.f12
			WHEN 13 THEN fp.f13
			WHEN 14 THEN fp.f14
			WHEN 15 THEN fp.f15
			WHEN 16 THEN fp.f16
			WHEN 17 THEN fp.f17
			WHEN 18 THEN fp.f18
			WHEN 19 THEN fp.f19
			WHEN 20 THEN fp.f20
			WHEN 21 THEN fp.f21
			WHEN 22 THEN fp.f22
			WHEN 23 THEN fp.f23
			WHEN 24 THEN fp.f24
			WHEN 25 THEN fp.f25
			WHEN 26 THEN fp.f26
			WHEN 27 THEN fp.f27
			WHEN 28 THEN fp.f28
			WHEN 29 THEN fp.f29
			WHEN 30 THEN fp.f30
			WHEN 31 THEN fp.f31
			WHEN 32 THEN fp.f32
			WHEN 33 THEN fp.f33
			WHEN 34 THEN fp.f34
			WHEN 35 THEN fp.f35
			WHEN 36 THEN fp.f36
			WHEN 37 THEN fp.f37
			WHEN 38 THEN fp.f38
			WHEN 39 THEN fp.f39
			WHEN 40 THEN fp.f40
			WHEN 41 THEN fp.f41
			WHEN 42 THEN fp.f42
			WHEN 43 THEN fp.f43
			WHEN 44 THEN fp.f44
			WHEN 45 THEN fp.f45
			WHEN 46 THEN fp.f46
			WHEN 47 THEN fp.f47
			WHEN 48 THEN fp.f48
			ELSE 0
		END
	)) as receita,
	SUM(fp.custo_na_data*CASE pb.tamanho
		WHEN 1 THEN fp.f1
		WHEN 2 THEN fp.f2
		WHEN 3 THEN fp.f3
		WHEN 4 THEN fp.f4
		WHEN 5 THEN fp.f5
		WHEN 6 THEN fp.f6
		WHEN 7 THEN fp.f7
		WHEN 8 THEN fp.f8
		WHEN 9 THEN fp.f9
		WHEN 10 THEN fp.f10
		WHEN 11 THEN fp.f11
		WHEN 12 THEN fp.f12
		WHEN 13 THEN fp.f13
		WHEN 14 THEN fp.f14
		WHEN 15 THEN fp.f15
		WHEN 16 THEN fp.f16
		WHEN 17 THEN fp.f17
		WHEN 18 THEN fp.f18
		WHEN 19 THEN fp.f19
		WHEN 20 THEN fp.f20
		WHEN 21 THEN fp.f21
		WHEN 22 THEN fp.f22
		WHEN 23 THEN fp.f23
		WHEN 24 THEN fp.f24
		WHEN 25 THEN fp.f25
		WHEN 26 THEN fp.f26
		WHEN 27 THEN fp.f27
		WHEN 28 THEN fp.f28
		WHEN 29 THEN fp.f29
		WHEN 30 THEN fp.f30
		WHEN 31 THEN fp.f31
		WHEN 32 THEN fp.f32
		WHEN 33 THEN fp.f33
		WHEN 34 THEN fp.f34
		WHEN 35 THEN fp.f35
		WHEN 36 THEN fp.f36
		WHEN 37 THEN fp.f37
		WHEN 38 THEN fp.f38
		WHEN 39 THEN fp.f39
		WHEN 40 THEN fp.f40
		WHEN 41 THEN fp.f41
		WHEN 42 THEN fp.f42
		WHEN 43 THEN fp.f43
		WHEN 44 THEN fp.f44
		WHEN 45 THEN fp.f45
		WHEN 46 THEN fp.f46
		WHEN 47 THEN fp.f47
		WHEN 48 THEN fp.f48
		ELSE 0
	END) as custo
	FROM faturamento_prod fp
	INNER JOIN faturamento f on
		f.nf_saida = fp.NF_SAIDA and
		fp.filial = f.filial
	INNER JOIN produtos_barra pb on
		fp.produto = pb.produto and
		fp.cor_produto = pb.cor_produto
	INNER JOIN produtos p on p.produto = fp.produto
	WHERE
	p.grupo_produto != 'GIFTCARD' and
	CASE pb.tamanho
		WHEN 1 THEN fp.f1
		WHEN 2 THEN fp.f2
		WHEN 3 THEN fp.f3
		WHEN 4 THEN fp.f4
		WHEN 5 THEN fp.f5
		WHEN 6 THEN fp.f6
		WHEN 7 THEN fp.f7
		WHEN 8 THEN fp.f8
		WHEN 9 THEN fp.f9
		WHEN 10 THEN fp.f10
		WHEN 11 THEN fp.f11
		WHEN 12 THEN fp.f12
		WHEN 13 THEN fp.f13
		WHEN 14 THEN fp.f14
		WHEN 15 THEN fp.f15
		WHEN 16 THEN fp.f16
		WHEN 17 THEN fp.f17
		WHEN 18 THEN fp.f18
		WHEN 19 THEN fp.f19
		WHEN 20 THEN fp.f20
		WHEN 21 THEN fp.f21
		WHEN 22 THEN fp.f22
		WHEN 23 THEN fp.f23
		WHEN 24 THEN fp.f24
		WHEN 25 THEN fp.f25
		WHEN 26 THEN fp.f26
		WHEN 27 THEN fp.f27
		WHEN 28 THEN fp.f28
		WHEN 29 THEN fp.f29
		WHEN 30 THEN fp.f30
		WHEN 31 THEN fp.f31
		WHEN 32 THEN fp.f32
		WHEN 33 THEN fp.f33
		WHEN 34 THEN fp.f34
		WHEN 35 THEN fp.f35
		WHEN 36 THEN fp.f36
		WHEN 37 THEN fp.f37
		WHEN 38 THEN fp.f38
		WHEN 39 THEN fp.f39
		WHEN 40 THEN fp.f40
		WHEN 41 THEN fp.f41
		WHEN 42 THEN fp.f42
		WHEN 43 THEN fp.f43
		WHEN 44 THEN fp.f44
		WHEN 45 THEN fp.f45
		WHEN 46 THEN fp.f46
		WHEN 47 THEN fp.f47
		WHEN 48 THEN fp.f48
		ELSE 0
	END > 0 and
	f.valor_sub_itens > 0 and
	fp.serie_nf IN (2,7) and
	f.natureza_saida = '100.01' and
	f.filial = 'e-commerce' and
	f.emissao BETWEEN GETDATE()-3*360 and GETDATE()-1-30
	GROUP BY
	CAST(f.emissao as DATE),
	pb.codigo_barra
) v
GROUP BY
v.data_venda,
v.codigo_barra;

IF OBJECT_ID('bi_potencial_produto_barra_temp') IS NOT NULL
  DROP TABLE bi_potencial_produto_barra_temp;
SELECT
v.*,
CASE
	WHEN v.potencial_itens_historico > 
		CASE
			WHEN v.potencial_itens_360 > 
				CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
			THEN v.potencial_itens_360 ELSE
				CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
		END
	THEN v.potencial_itens_historico ELSE
		CASE
			WHEN v.potencial_itens_360 > 
				CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
			THEN v.potencial_itens_360 ELSE
				CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
		END
END as potencial_itens,
CASE 
	CASE
		WHEN v.potencial_itens_historico > 
			CASE
				WHEN v.potencial_itens_360 > 
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
				THEN v.potencial_itens_360 ELSE
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
			END
		THEN v.potencial_itens_historico ELSE
			CASE
				WHEN v.potencial_itens_360 > 
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
				THEN v.potencial_itens_360 ELSE
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
			END
	END
	WHEN v.potencial_itens_historico THEN v.potencial_receita_historico
	WHEN v.potencial_itens_360 THEN v.potencial_receita_360
	WHEN v.potencial_itens_90 THEN v.potencial_receita_90
	WHEN v.potencial_itens_30 THEN v.potencial_receita_30
END as potencial_receita,
CASE 
	CASE
		WHEN v.potencial_itens_historico > 
			CASE
				WHEN v.potencial_itens_360 > 
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
				THEN v.potencial_itens_360 ELSE
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
			END
		THEN v.potencial_itens_historico ELSE
			CASE
				WHEN v.potencial_itens_360 > 
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
				THEN v.potencial_itens_360 ELSE
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
			END
	END
	WHEN v.potencial_itens_historico THEN v.potencial_pc1_historico
	WHEN v.potencial_itens_360 THEN v.potencial_pc1_360
	WHEN v.potencial_itens_90 THEN v.potencial_pc1_90
	WHEN v.potencial_itens_30 THEN v.potencial_pc1_30
END as potencial_pc1
INTO dbo.bi_potencial_produto_barra_temp
FROM (
	SELECT
	v.codigo_barra,
	v.produto,
	v.cor_produto,
	v.tamanho,

	COALESCE(v.itens_historico/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_historico,v.ultima_venda_historico) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_historico,v.ultima_venda_historico) END AS FLOAT),0),0) as potencial_itens_historico,
	COALESCE(v.receita_historico/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_historico,v.ultima_venda_historico) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_historico,v.ultima_venda_historico) END AS FLOAT),0),0) as potencial_receita_historico,
	COALESCE((v.receita_historico-v.custo_historico)/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_historico,v.ultima_venda_historico) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_historico,v.ultima_venda_historico) END AS FLOAT),0),0) as potencial_pc1_historico,

	COALESCE(v.itens_360/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_360,v.ultima_venda_360) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_360,v.ultima_venda_360) END AS FLOAT),0),0) as potencial_itens_360,
	COALESCE(v.receita_360/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_360,v.ultima_venda_360) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_360,v.ultima_venda_360) END AS FLOAT),0),0) as potencial_receita_360,
	COALESCE((v.receita_360-v.custo_360)/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_360,v.ultima_venda_360) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_360,v.ultima_venda_360) END AS FLOAT),0),0) as potencial_pc1_360,

	COALESCE(v.itens_90/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_90,v.ultima_venda_90) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_90,v.ultima_venda_90) END AS FLOAT),0),0) as potencial_itens_90,
	COALESCE(v.receita_90/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_90,v.ultima_venda_90) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_90,v.ultima_venda_90) END AS FLOAT),0),0) as potencial_receita_90,
	COALESCE((v.receita_90-v.custo_90)/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_90,v.ultima_venda_90) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_90,v.ultima_venda_90) END AS FLOAT),0),0) as potencial_pc1_90,

	COALESCE(v.itens_30/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) < 7 THEN 7 ELSE DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) END AS FLOAT),0),0) as potencial_itens_30,
	COALESCE(v.receita_30/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) < 7 THEN 7 ELSE DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) END AS FLOAT),0),0) as potencial_receita_30,
	COALESCE((v.receita_30-v.custo_30)/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) < 7 THEN 7 ELSE DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) END AS FLOAT),0),0) as potencial_pc1_30,

	COALESCE(v.receita_sorting/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) < 7 THEN 7 ELSE DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) END AS FLOAT),0),0) as potencial_receita_sorting
	FROM (
		SELECT
		v.codigo_barra,
		MAX(v.produto) as produto,
		MAX(v.cor_produto) as cor_produto,
		MAX(v.tamanho) as tamanho,

		MIN(v.data_venda) as primeira_venda_historico,
		MAX(v.data_venda) as ultima_venda_historico,
		SUM(v.itens) as itens_historico,
		SUM(v.receita) as receita_historico,
		SUM(v.custo) as custo_historico,
		MIN(CASE WHEN v.data_venda >= GETDATE()-360 THEN v.data_venda END) as primeira_venda_360,
		MAX(CASE WHEN v.data_venda >= GETDATE()-360 THEN v.data_venda END) as ultima_venda_360,
		SUM(CASE WHEN v.data_venda >= GETDATE()-360 THEN v.itens ELSE 0 END) as itens_360,
		SUM(CASE WHEN v.data_venda >= GETDATE()-360 THEN v.receita ELSE 0 END) as receita_360,
		SUM(CASE WHEN v.data_venda >= GETDATE()-360 THEN v.custo ELSE 0 END) as custo_360,

		MIN(CASE WHEN v.data_venda >= GETDATE()-90 THEN v.data_venda END) as primeira_venda_90,
		MAX(CASE WHEN v.data_venda >= GETDATE()-90 THEN v.data_venda END) as ultima_venda_90,
		SUM(CASE WHEN v.data_venda >= GETDATE()-90 THEN v.itens ELSE 0 END) as itens_90,
		SUM(CASE WHEN v.data_venda >= GETDATE()-90 THEN v.receita ELSE 0 END) as receita_90,
		SUM(CASE WHEN v.data_venda >= GETDATE()-90 THEN v.custo ELSE 0 END) as custo_90,

		MIN(CASE WHEN v.data_venda >= GETDATE()-30 THEN v.data_venda END) as primeira_venda_30,
		MAX(CASE WHEN v.data_venda >= GETDATE()-30 THEN v.data_venda END) as ultima_venda_30,
		SUM(CASE WHEN v.data_venda >= GETDATE()-30 THEN v.itens ELSE 0 END) as itens_30,
		SUM(CASE WHEN v.data_venda >= GETDATE()-30 THEN v.receita ELSE 0 END) as receita_30,
		SUM(CASE WHEN v.data_venda >= GETDATE()-30 THEN v.custo ELSE 0 END) as custo_30,

		SUM(CASE WHEN v.data_venda >= GETDATE()-30 THEN v.receita_sorting ELSE 0 END) as receita_sorting
		FROM potencial_venda_produto_barra_dia v
		GROUP BY
		v.codigo_barra
	) v
) v;

IF OBJECT_ID('bi_potencial_produto_cor_temp') IS NOT NULL
  DROP TABLE bi_potencial_produto_cor_temp;
SELECT
v.*,
CASE
	WHEN v.potencial_itens_historico > 
		CASE
			WHEN v.potencial_itens_360 > 
				CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
			THEN v.potencial_itens_360 ELSE
				CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
		END
	THEN v.potencial_itens_historico ELSE
		CASE
			WHEN v.potencial_itens_360 > 
				CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
			THEN v.potencial_itens_360 ELSE
				CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
		END
END as potencial_itens,
CASE 
	CASE
		WHEN v.potencial_itens_historico > 
			CASE
				WHEN v.potencial_itens_360 > 
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
				THEN v.potencial_itens_360 ELSE
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
			END
		THEN v.potencial_itens_historico ELSE
			CASE
				WHEN v.potencial_itens_360 > 
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
				THEN v.potencial_itens_360 ELSE
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
			END
	END
	WHEN v.potencial_itens_historico THEN v.potencial_receita_historico
	WHEN v.potencial_itens_360 THEN v.potencial_receita_360
	WHEN v.potencial_itens_90 THEN v.potencial_receita_90
	WHEN v.potencial_itens_30 THEN v.potencial_receita_30
END as potencial_receita,
CASE 
	CASE
		WHEN v.potencial_itens_historico > 
			CASE
				WHEN v.potencial_itens_360 > 
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
				THEN v.potencial_itens_360 ELSE
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
			END
		THEN v.potencial_itens_historico ELSE
			CASE
				WHEN v.potencial_itens_360 > 
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
				THEN v.potencial_itens_360 ELSE
					CASE WHEN v.potencial_itens_90 > v.potencial_itens_30 THEN v.potencial_itens_90 ELSE v.potencial_itens_30 END
			END
	END
	WHEN v.potencial_itens_historico THEN v.potencial_pc1_historico
	WHEN v.potencial_itens_360 THEN v.potencial_pc1_360
	WHEN v.potencial_itens_90 THEN v.potencial_pc1_90
	WHEN v.potencial_itens_30 THEN v.potencial_pc1_30
END as potencial_pc1
INTO dbo.bi_potencial_produto_cor_temp
FROM (
	SELECT
	v.produto,
	v.cor_produto,

	COALESCE(v.itens_historico/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_historico,v.ultima_venda_historico) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_historico,v.ultima_venda_historico) END AS FLOAT),0),0) as potencial_itens_historico,
	COALESCE(v.receita_historico/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_historico,v.ultima_venda_historico) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_historico,v.ultima_venda_historico) END AS FLOAT),0),0) as potencial_receita_historico,
	COALESCE((v.receita_historico-v.custo_historico)/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_historico,v.ultima_venda_historico) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_historico,v.ultima_venda_historico) END AS FLOAT),0),0) as potencial_pc1_historico,

	COALESCE(v.itens_360/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_360,v.ultima_venda_360) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_360,v.ultima_venda_360) END AS FLOAT),0),0) as potencial_itens_360,
	COALESCE(v.receita_360/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_360,v.ultima_venda_360) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_360,v.ultima_venda_360) END AS FLOAT),0),0) as potencial_receita_360,
	COALESCE((v.receita_360-v.custo_360)/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_360,v.ultima_venda_360) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_360,v.ultima_venda_360) END AS FLOAT),0),0) as potencial_pc1_360,

	COALESCE(v.itens_90/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_90,v.ultima_venda_90) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_90,v.ultima_venda_90) END AS FLOAT),0),0) as potencial_itens_90,
	COALESCE(v.receita_90/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_90,v.ultima_venda_90) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_90,v.ultima_venda_90) END AS FLOAT),0),0) as potencial_receita_90,
	COALESCE((v.receita_90-v.custo_90)/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_90,v.ultima_venda_90) < 15 THEN 15 ELSE DATEDIFF(DAY,v.primeira_venda_90,v.ultima_venda_90) END AS FLOAT),0),0) as potencial_pc1_90,

	COALESCE(v.itens_30/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) < 7 THEN 7 ELSE DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) END AS FLOAT),0),0) as potencial_itens_30,
	COALESCE(v.receita_30/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) < 7 THEN 7 ELSE DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) END AS FLOAT),0),0) as potencial_receita_30,
	COALESCE((v.receita_30-v.custo_30)/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) < 7 THEN 7 ELSE DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) END AS FLOAT),0),0) as potencial_pc1_30,
	
	COALESCE(v.receita_sorting/NULLIF(CAST(CASE WHEN DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) < 7 THEN 7 ELSE DATEDIFF(DAY,v.primeira_venda_30,v.ultima_venda_30) END AS FLOAT),0),0) as potencial_receita_sorting
	FROM (
		SELECT
		v.produto,
		v.cor_produto,

		MIN(v.data_venda) as primeira_venda_historico,
		MAX(v.data_venda) as ultima_venda_historico,
		SUM(v.itens) as itens_historico,
		SUM(v.receita) as receita_historico,
		SUM(v.custo) as custo_historico,
		MIN(CASE WHEN v.data_venda >= GETDATE()-360 THEN v.data_venda END) as primeira_venda_360,
		MAX(CASE WHEN v.data_venda >= GETDATE()-360 THEN v.data_venda END) as ultima_venda_360,
		SUM(CASE WHEN v.data_venda >= GETDATE()-360 THEN v.itens ELSE 0 END) as itens_360,
		SUM(CASE WHEN v.data_venda >= GETDATE()-360 THEN v.receita ELSE 0 END) as receita_360,
		SUM(CASE WHEN v.data_venda >= GETDATE()-360 THEN v.custo ELSE 0 END) as custo_360,

		MIN(CASE WHEN v.data_venda >= GETDATE()-90 THEN v.data_venda END) as primeira_venda_90,
		MAX(CASE WHEN v.data_venda >= GETDATE()-90 THEN v.data_venda END) as ultima_venda_90,
		SUM(CASE WHEN v.data_venda >= GETDATE()-90 THEN v.itens ELSE 0 END) as itens_90,
		SUM(CASE WHEN v.data_venda >= GETDATE()-90 THEN v.receita ELSE 0 END) as receita_90,
		SUM(CASE WHEN v.data_venda >= GETDATE()-90 THEN v.custo ELSE 0 END) as custo_90,

		MIN(CASE WHEN v.data_venda >= GETDATE()-30 THEN v.data_venda END) as primeira_venda_30,
		MAX(CASE WHEN v.data_venda >= GETDATE()-30 THEN v.data_venda END) as ultima_venda_30,
		SUM(CASE WHEN v.data_venda >= GETDATE()-30 THEN v.itens ELSE 0 END) as itens_30,
		SUM(CASE WHEN v.data_venda >= GETDATE()-30 THEN v.receita ELSE 0 END) as receita_30,
		SUM(CASE WHEN v.data_venda >= GETDATE()-30 THEN v.custo ELSE 0 END) as custo_30,

		SUM(CASE WHEN v.data_venda >= GETDATE()-30 THEN v.receita_sorting ELSE 0 END) as receita_sorting
		FROM potencial_venda_produto_barra_dia v
		GROUP BY
		v.produto,
		v.cor_produto
	) v
) v;

IF OBJECT_ID('potencial_classificacao_abc') IS NOT NULL
  DROP TABLE potencial_classificacao_abc;
WITH collections as (
	SELECT
	CASE WHEN MONTH(GETDATE()-1) BETWEEN 2 and 7 THEN 'OI' ELSE 'V' END +
	CAST(CASE WHEN MONTH(GETDATE()-1) BETWEEN 2 and 7 THEN YEAR(GETDATE()-1) ELSE YEAR(GETDATE()-1)+1 END AS CHAR) as collection
	UNION
	SELECT
	CASE WHEN MONTH(GETDATE()-1) BETWEEN 2 and 7 THEN 'OI' ELSE 'V' END +
	CAST(CASE WHEN MONTH(GETDATE()-1) BETWEEN 2 and 7 THEN YEAR(GETDATE()-1)-1 ELSE YEAR(GETDATE()-1) END AS CHAR) as collection
	UNION
	SELECT
	CASE WHEN MONTH(GETDATE()-1) BETWEEN 2 and 7 THEN 'V' ELSE 'OI' END +
	CAST(YEAR(GETDATE()-1) AS CHAR) as collection
),
base as (
	SELECT
	ppc.produto,
	ppc.cor_produto,
	COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros') as categoria,
	--p.fabricante,
	CASE
		WHEN c.preco1 < 20 THEN '01. 0-20'
		WHEN c.preco1 < 40 THEN '02. 20-40'
		WHEN c.preco1 < 60 THEN '03. 40-60'
		WHEN c.preco1 < 80 THEN '04. 60-80'
		WHEN c.preco1 < 100 THEN '05. 80-100'
		WHEN c.preco1 < 125 THEN '06. 100-125'
		WHEN c.preco1 < 150 THEN '07. 125-150'
		WHEN c.preco1 < 175 THEN '08. 150-175'
		WHEN c.preco1 < 200 THEN '09. 175-200'
		WHEN c.preco1 < 250 THEN '10. 200-250'
		WHEN c.preco1 < 300 THEN '11. 250-300'
		WHEN c.preco1 < 400 THEN '12. 300-400'
		WHEN c.preco1 < 500 THEN '13. 400-500'
		ELSE '14. 500+'
	END as faixa_de_custo,
	ROW_NUMBER() OVER(PARTITION BY 
		COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros'),
		--p.fabricante,
		CASE
			WHEN c.preco1 < 20 THEN '01. 0-20'
			WHEN c.preco1 < 40 THEN '02. 20-40'
			WHEN c.preco1 < 60 THEN '03. 40-60'
			WHEN c.preco1 < 80 THEN '04. 60-80'
			WHEN c.preco1 < 100 THEN '05. 80-100'
			WHEN c.preco1 < 125 THEN '06. 100-125'
			WHEN c.preco1 < 150 THEN '07. 125-150'
			WHEN c.preco1 < 175 THEN '08. 150-175'
			WHEN c.preco1 < 200 THEN '09. 175-200'
			WHEN c.preco1 < 250 THEN '10. 200-250'
			WHEN c.preco1 < 300 THEN '11. 250-300'
			WHEN c.preco1 < 400 THEN '12. 300-400'
			WHEN c.preco1 < 500 THEN '13. 400-500'
			ELSE '14. 500+'
		END
		ORDER BY ppc.potencial_pc1 DESC) as numero,
	ppc.potencial_itens
	FROM bi_potencial_produto_cor_temp ppc
	INNER JOIN produtos p on p.produto = ppc.produto
	INNER JOIN produtos_precos c on
		p.produto = c.produto and
		c.codigo_tab_preco = 02 -- 02 custo
	LEFT JOIN bi_categorizacao_categoria ccc on
		ccc.grupo_produto = p.grupo_produto and
		ccc.cod_categoria = p.cod_categoria and
		ccc.subgrupo_produto is null and
		ccc.tipo_produto is null
	LEFT JOIN bi_categorizacao_categoria cct on
		cct.grupo_produto = p.grupo_produto and
		cct.tipo_produto = p.tipo_produto and
		cct.subgrupo_produto is null and
		cct.cod_categoria is null
	LEFT JOIN bi_categorizacao_categoria ccs on
		ccs.grupo_produto = p.grupo_produto and
		ccs.subgrupo_produto = p.subgrupo_produto and
		ccs.tipo_produto is null and
		ccs.cod_categoria is null
	LEFT JOIN bi_categorizacao_categoria cc on
		cc.grupo_produto = p.grupo_produto and
		cc.subgrupo_produto is null and
		cc.tipo_produto is null and
		cc.cod_categoria is null
	WHERE
	p.colecao IN (SELECT * FROM collections) and
	ppc.potencial_pc1 > 0
)
SELECT
b.produto,
b.cor_produto,
CASE
	WHEN m.modelos is null THEN ''
	WHEN b.numero/CAST(m.modelos as FLOAT) <= 0.2 THEN 'A'
	WHEN b.numero/CAST(m.modelos as FLOAT) <= 0.5 THEN 'B'
	ELSE 'C'
END as curva_abc
INTO dbo.potencial_classificacao_abc
FROM base b
LEFT JOIN (
	SELECT
	b.categoria,
	--b.fabricante,
	b.faixa_de_custo,
	MAX(numero) as modelos
	FROM base b
	GROUP BY
	b.categoria,
	--b.fabricante,
	b.faixa_de_custo
	HAVING
	COUNT(*) > 5
) m on 
	b.categoria = m.categoria and
	--b.fabricante = m.fabricante and
	b.faixa_de_custo = m.faixa_de_custo;

IF OBJECT_ID('bi_potencial_produto_barra') IS NOT NULL
  DROP TABLE bi_potencial_produto_barra;
CREATE TABLE dbo.bi_potencial_produto_barra (
	codigo_barra VARCHAR(50) NOT NULL,
	produto VARCHAR(50) DEFAULT NULL,
	cor_produto VARCHAR(50) DEFAULT NULL,
	tamanho VARCHAR(50) DEFAULT NULL,
	potencial_itens_historico FLOAT NOT NULL,
	potencial_receita_historico FLOAT NOT NULL,
	potencial_pc1_historico FLOAT NOT NULL,
	potencial_itens_360 FLOAT NOT NULL,
	potencial_receita_360 FLOAT NOT NULL,
	potencial_pc1_360 FLOAT NOT NULL,
	potencial_itens_90 FLOAT NOT NULL,
	potencial_receita_90 FLOAT NOT NULL,
	potencial_pc1_90 FLOAT NOT NULL,
	potencial_itens_30 FLOAT NOT NULL,
	potencial_receita_30 FLOAT NOT NULL,
	potencial_pc1_30 FLOAT NOT NULL,
	potencial_receita_sorting FLOAT NOT NULL,
	potencial_itens FLOAT NOT NULL,
	potencial_receita FLOAT NOT NULL,
	potencial_pc1 FLOAT NOT NULL,
	curva_abc VARCHAR(50) NOT NULL,
	PRIMARY KEY (codigo_barra)
);
INSERT INTO bi_potencial_produto_barra
SELECT
ppb.*,
COALESCE(abc.curva_abc,'') as curva_abc
FROM bi_potencial_produto_barra_temp ppb
LEFT JOIN potencial_classificacao_abc abc on
	abc.produto = ppb.produto and
	abc.cor_produto = ppb.cor_produto;

IF OBJECT_ID('bi_potencial_produto_cor') IS NOT NULL
  DROP TABLE bi_potencial_produto_cor;
CREATE TABLE dbo.bi_potencial_produto_cor (
	produto VARCHAR(50) DEFAULT NULL,
	cor_produto VARCHAR(50) DEFAULT NULL,
	potencial_itens_historico FLOAT NOT NULL,
	potencial_receita_historico FLOAT NOT NULL,
	potencial_pc1_historico FLOAT NOT NULL,
	potencial_itens_360 FLOAT NOT NULL,
	potencial_receita_360 FLOAT NOT NULL,
	potencial_pc1_360 FLOAT NOT NULL,
	potencial_itens_90 FLOAT NOT NULL,
	potencial_receita_90 FLOAT NOT NULL,
	potencial_pc1_90 FLOAT NOT NULL,
	potencial_itens_30 FLOAT NOT NULL,
	potencial_receita_30 FLOAT NOT NULL,
	potencial_pc1_30 FLOAT NOT NULL,
	potencial_receita_sorting FLOAT NOT NULL,
	potencial_itens FLOAT NOT NULL,
	potencial_receita FLOAT NOT NULL,
	potencial_pc1 FLOAT NOT NULL,
	curva_abc VARCHAR(50) NOT NULL,
	PRIMARY KEY (produto, cor_produto)
);
INSERT INTO bi_potencial_produto_cor
SELECT
ppc.*,
COALESCE(abc.curva_abc,'') as curva_abc
FROM bi_potencial_produto_cor_temp ppc
LEFT JOIN potencial_classificacao_abc abc on
	abc.produto = ppc.produto and
	abc.cor_produto = ppc.cor_produto;

IF OBJECT_ID('potencial_venda_produto_barra_dia') IS NOT NULL
  DROP TABLE potencial_venda_produto_barra_dia;
IF OBJECT_ID('bi_potencial_produto_barra_temp') IS NOT NULL
  DROP TABLE bi_potencial_produto_barra_temp;
IF OBJECT_ID('bi_potencial_produto_cor_temp') IS NOT NULL
  DROP TABLE bi_potencial_produto_cor_temp;
IF OBJECT_ID('potencial_classificacao_abc') IS NOT NULL
  DROP TABLE potencial_classificacao_abc;
