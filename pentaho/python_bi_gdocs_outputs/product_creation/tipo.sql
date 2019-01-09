SELECT DISTINCT 
t.tipo_produto,
CASE WHEN t.tipo_produto IN ('Crepe','Viscose','Linho','Jeans','Malha','Couro','Sarja','Alfaiataria','Sued','Tricot','Chiffon') THEN t.tipo_produto ELSE '' END as grupo_tipo
FROM (
	SELECT DISTINCT
	RTRIM(p.tipo_produto) as tipo_produto
	FROM produtos p
	WHERE
	(p.grupo_produto like '[_]%' or p.grupo_produto = 'CALCADOS')
	and p.data_cadastramento >= GETDATE()-360
	
	UNION

	SELECT DISTINCT
	RTRIM(pt.tipo_produto) as tipo_produto
	FROM produtos_tipos pt
	WHERE
	pt.inativo = 0 and
	(
		pt.data_para_transferencia >= GETDATE()-360 
		OR pt.tipo_produto = 'POLIAMIDA'
	)
) t
ORDER BY 
t.tipo_produto