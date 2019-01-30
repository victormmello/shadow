#!/usr/bin/env python
# -*- coding: utf-8 -*-

import lxml.etree
import xmlsec
import urllib2
from lxml import etree

###############################################################################
# Ajustes para NFe
###############################################################################
# Devido ao nome da chave no Brasil ter "/" e ":",
# no arquivo pk11.py, colocar no início "import urllib2" e abaixo da linha 47:
# (module_path, sep, keyqs) = o.path.rpartition('/')
# colocar a linha:
# keyqs = urllib2.unquote(keyqs)
# Alguns cartões não tem especificação de nome da chave, assim para funcionar
# deve-se retirar (CKA_LABEL, keyname) na linha 135:
# key = _find_object(session, [(CKA_LABEL, keyname), (CKA_CLASS, ...
###############################################################################
# O ID slot deve vir da lista de slots, assim, no arquivo pk11.py, trocar:
# session = lib.openSession(slot) # linha 164
# por:
# session = lib.openSession(lib.getSlotList()[slot])
###############################################################################
# O atributo reference_uri no NFe é dado por "Id", mas o xmlsec procura por
# "id" e "ID", não encontrando o da NFe, assim deve-se adicionar este em
# __init__.py, na linha 48:
# id_attributes = pyconfig.setting("xmlsec.id_attributes", ['ID', 'id'])
# ficando:
# id_attributes = pyconfig.setting("xmlsec.id_attributes", ['ID', 'id', 'Id'])
###############################################################################
# Para sair o certificado na assinatura, é preciso, no arquivo __init__.py,
# na função _load_keyspec, abaixo da linha 165:
# 'source': 'pkcs11',
# colocar a linha:
# 'data': data,
# e depois, na função sign, trocar a linha 551:
# public = None
# por:
# public = private if private['source'] == 'pkcs11' else None
###############################################################################

# Leitora Gemalto de R$ 25,00 compatívle com Linux
# Usando lib da A.E.T. Europe B.V., do pacote safesignidentityclient 3.0.88-12
# para Debian. Essa versão é importante para suporte ao cartão Certisign 2015
# O keyname, na minha leitora de cartão, é o label do Private Key obtido com:
# pkcs11-tool -O -l --module /usr/lib/libaetpkss.so
module = 'C:\\Windows\\System32\\aetpkss1.dll'
# module = '/usr/lib/libaetpkss.so'
keyname = 'RAFAEL COUTINHO DE MELO SERRANO EIRELI EPP:07400225000184'
pin = '2017'

# Dados da NFe para assinar
chave = '35190107400225000184550020000067271182139170'
# nfe = '/tmp/%s-nfe.xml' % chave  # caminho para a NFe da chave acima
reference_uri = '#NFe' + chave
# xml = lxml.etree.parse(nfe)
xml_str = """<NFe xmlns="http://www.portalfiscal.inf.br/nfe"><infNFe versao="4.00" Id="NFe35190107400225000184550020000067271182139170"><ide><cUF>35</cUF><cNF>18213917</cNF><natOp>VENDA DE MERCADORIA</natOp><mod>55</mod><serie>2</serie><nNF>6727</nNF><dhEmi>2019-01-10T09:18:00-04:00</dhEmi><dhSaiEnt>2019-01-10T09:18:00-04:00</dhSaiEnt><tpNF>1</tpNF><idDest>1</idDest><cMunFG>3509502</cMunFG><tpImp>1</tpImp><tpEmis>1</tpEmis><cDV>0</cDV><tpAmb>1</tpAmb><finNFe>1</finNFe><indFinal>1</indFinal><indPres>0</indPres><procEmi>0</procEmi><verProc>LinxERP8111836</verProc></ide><emit><CNPJ>07400225000184</CNPJ><xNome>RAFAEL COUTINHO DE MELO SERRANO EIRELI</xNome><xFant>ECOMMERCE</xFant><enderEmit><xLgr>R SANTOS DUMONT</xLgr><nro>845</nro><xBairro>CAMBUI</xBairro><cMun>3509502</cMun><xMun>CAMPINAS</xMun><UF>SP</UF><CEP>13024021</CEP><cPais>1058</cPais><xPais>BRASIL</xPais></enderEmit><IE>795989794119</IE><CRT>3</CRT></emit><dest><CPF>25884285841</CPF><xNome>ROSELI AMORIM</xNome><enderDest><xLgr>RUA SANTOS DUMONT</xLgr><nro>845</nro><xCpl>LOJA</xCpl><xBairro>CAMBUI</xBairro><cMun>3509502</cMun><xMun>CAMPINAS</xMun><UF>SP</UF><CEP>13024021</CEP><cPais>1058</cPais><xPais>BRASIL</xPais><fone>19983983440</fone></enderDest><indIEDest>9</indIEDest><email>roseli.leonardo_@hotmail.com</email></dest><det nItem="1"><prod><cProd>28.01.0052</cProd><cEAN>SEM GTIN</cEAN><xProd>CHEMISIE FRANZIDO BRENTWOOD</xProd><NCM>61044400</NCM><CFOP>5102</CFOP><uCom>PC</uCom><qCom>1.0000</qCom><vUnCom>74.9900000000</vUnCom><vProd>74.99</vProd><cEANTrib>SEM GTIN</cEANTrib><uTrib>PC</uTrib><qTrib>1.0000</qTrib><vUnTrib>74.9900000000</vUnTrib><indTot>1</indTot></prod><imposto><vTotTrib>10.09</vTotTrib><ICMS><ICMS00><orig>0</orig><CST>00</CST><modBC>3</modBC><vBC>74.99</vBC><pICMS>18.0000</pICMS><vICMS>13.49</vICMS></ICMS00></ICMS><IPI><cEnq>999</cEnq><IPINT><CST>53</CST></IPINT></IPI><PIS><PISAliq><CST>01</CST><vBC>74.99</vBC><pPIS>0.6500</pPIS><vPIS>0.49</vPIS></PISAliq></PIS><COFINS><COFINSAliq><CST>01</CST><vBC>74.99</vBC><pCOFINS>3.0000</pCOFINS><vCOFINS>2.25</vCOFINS></COFINSAliq></COFINS></imposto><infAdProd>.   Trib. Aprox. R$: 10.09 Federal e 13.50 Estadual FONTE: IBPT/empresometro.com.br  D529CB</infAdProd></det><total><ICMSTot><vBC>74.99</vBC><vICMS>13.49</vICMS><vICMSDeson>0.00</vICMSDeson><vICMSUFDest>0.00</vICMSUFDest><vICMSUFRemet>0.00</vICMSUFRemet><vFCP>0.00</vFCP><vBCST>0.00</vBCST><vST>0.00</vST><vFCPST>0.00</vFCPST><vFCPSTRet>0.00</vFCPSTRet><vProd>74.99</vProd><vFrete>0.00</vFrete><vSeg>0.00</vSeg><vDesc>0.00</vDesc><vII>0.00</vII><vIPI>0.00</vIPI><vIPIDevol>0.00</vIPIDevol><vPIS>0.49</vPIS><vCOFINS>2.25</vCOFINS><vOutro>0.00</vOutro><vNF>74.99</vNF><vTotTrib>10.09</vTotTrib></ICMSTot></total><transp><modFrete>1</modFrete><transporta><CNPJ>34028316003129</CNPJ><xNome>NORMAL (16ED1F7)</xNome><xMun>CAMPINAS</xMun><UF>SP</UF></transporta><vol><qVol>1</qVol><esp>CAIXA DE PAPELAO</esp></vol></transp><pag><detPag><indPag>1</indPag><tPag>99</tPag><vPag>74.99</vPag></detPag></pag><infAdic><infCpl>TRIB. APROX. R$: 10.09 FEDERAL E 13.50 ESTADUAL FONTE: IBPT/EMPRESOMETRO.COM.BR    D529CB</infCpl></infAdic></infNFe></NFe>"""
xml = etree.fromstring(xml_str)
# Tags XML da assinatura conforme padrão da NFe
transforms = (xmlsec.constants.TRANSFORM_ENVELOPED_SIGNATURE,
              xmlsec.constants.TRANSFORM_C14N_INCLUSIVE)
xmlsec.add_enveloped_signature(xml,
                               transforms=transforms,
                               reference_uri=reference_uri,
                               pos=-1)

# Especificação para usar o A3

keyname = urllib2.quote(keyname, '')
pk11_uri = 'pkcs11://%s/%s?pin=%s' % (module, keyname, pin)

# Assinando a NFe com A3
a,cert, chave = xmlsec.sign(xml, pk11_uri)
print(chave)
# Salvando a NFe assinada
import pdb; pdb.set_trace()
xml.write(nfe[:-3] + '-assinada.xml',
          encoding=xml.docinfo.encoding,
          xml_declaration=True)

# print(xml)