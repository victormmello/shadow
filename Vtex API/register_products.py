from database.database_connection import DatabaseConnection
dc = DatabaseConnection()

products_to_register = dc.select("""
	SELECT distinct
		-- count(*), sum(e.estoque_disponivel)
		-- , p.DATA_REPOSICAO 
		p.produto as prod_code, 
		ps.grade as size, 
		ps.COR_PRODUTO as cod_color, 
		pc.DESC_COR_PRODUTO as desc_color, 
		color.vtex_color as vtex_color,
		ps.CODIGO_BARRA as ean,
		'2000000' as brand_id,
		COALESCE(cat1.vtex_category_id, cat2.vtex_category_id) as vtex_category_id,
		COALESCE(cat1.vtex_department_id, cat2.vtex_department_id) as vtex_department_id,
		p.DATA_REPOSICAO
	from dbo.PRODUTOS_BARRA ps
	inner join dbo.PRODUTOS p on p.produto = ps.produto
	inner join dbo.PRODUTO_CORES pc on pc.produto = p.produto and ps.COR_PRODUTO = pc.COR_PRODUTO
	inner join w_estoque_disponivel_sku e on e.codigo_barra = ps.CODIGO_BARRA and e.filial = 'e-commerce'
	left join dbo.bi_vtex_product_items v_item on v_item.ean = ps.codigo_barra
	left join bi_vtex_categorizacao_categoria cat1 on cat1.grupo_produto = p.GRUPO_PRODUTO and cat1.subgrupo_produto = p.SUBGRUPO_PRODUTO
	left join bi_vtex_categorizacao_categoria cat2 on cat2.grupo_produto = p.GRUPO_PRODUTO and cat2.subgrupo_produto is null
	left join bi_vtex_categorizacao_cor color on color.linx_color = pc.DESC_COR_PRODUTO
	where 1=1
		and p.DATA_REPOSICAO > '2018-01-01'
		and v_item.image_url is null
		-- and p.produto = '33.02.0220'
		and e.estoque_disponivel > 0
	order by p.DATA_REPOSICAO
	;
""", trim=True)

for product_info in products_to_register:
	print(product_info)

	product_info['prod_code']
	product_info['size']
	product_info['cod_color']
	product_info['desc_color']
	product_info['vtex_color']
	product_info['ean']
	product_info['brand_id']
	product_info['vtex_category_id']
	product_info['vtex_department_id']