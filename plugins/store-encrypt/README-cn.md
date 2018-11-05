# ֧��Elasticsearch������ݼ���

[TOC]

## ֧��������ݼ��ܵĲ��

��ElasticSearch�������У��洢�е�������д��־ô洢ʱ���ᱻ���ܡ����������ɻ����У������зǳ��ߵ����ݰ�ȫҪ��Ϊ�ˣ����ǿ�����һ���������ݼ��ܵĴ洢���������֧�ֶ�������ݵ����в��ֽ������ݼ��ܡ���������������Ҫ�����㣬�ǿ��ǵ�lucene��Դ�����������۹�������ݰ�ȫ��һ��������ͨ���ļ�ϵͳ���ܣ�����������������Ӱ��ϴ�Ϊ�ˣ������ۺϿ��������ܺ�������ݰ�ȫ�Ĳ��ص㣬�Էǳ����е�������ݽ��м��ܣ�����tim��fdt��dvd�⼸�ְ���������Ϣ���ļ���ͬʱ�������������ļ������ܡ�


## ���

Ϊ�˱���������ݱ������϶���򱸷���ȡ�� ���ǿ�����Ƽ��ܵ���½���ݴ洢�����û�������Կ���½���ݷֿ��������� �����ڵ��ֻ��������ϸ�������֤��Ȩ��ͨ���빫˾���ڲ���Կ��ȫ������أ�����ܻ�ȡ����Կ�� Ϊ�ˣ�������Ʋ�ʵ����Store-encrypt�������֧�ֵ�½���ݼ��ܺ���Կ�Խӹ���

���ܰ���

* ֧��AES-ECBģʽ���ܣ�128bit��
* ֧��2����Կ���ݷ�����������Կ�洢��index settings�������ܵ�������Կ�洢�� index settings��֧�ֶԽ�����Կ
* ����Elasticsearch��ѡĿ¼��������
* ���ģʽ��������ES��lucene����


## ��ʼ Getting Started

ͨ�����¼�������Ϳ�������������ݼ��ܵ�������

* ���루��ѡ������ֱ�����ض�Ӧ�İ汾plugin
* ��װ
* ����

����ο�����

## ���� 

���ȣ���Ҫ�Ӳֿ����ظò����������Elasticsearch��Ŀ�������˵������ο�Elasticsearch�ı������̡�������ɺ󣬽���elasticsearch�ĸ�Ŀ¼��ִ���������������ɱ��룺

	gradlew assemble


���������룬�������ض�Ӧ�İ汾ֱ�Ӱ�װ���ɡ�����ο����µķ����汾��
��ѡ�汾��

* [Plugin For Elasticsearch 6.3.2](http://)

## ��װ Installation

��ο�  ElasticSearch �Ĳ����װָ�������Դ������òο�
https://www.elastic.co/guide/en/elasticsearch/plugins/6.4/plugin-management-custom-url.html

## ���� Configuration 

1.����Elasticsearch��lucene�������ļ��ϲ�ģʽ����ѡ��
		
		setUseCompoundFile

2.���ô洢����store type�� index.store.type 

Ĭ������£�Elasticsearch֧�ֿ�ѡ���ļ��洢������ fs��niofs��mmapfs��simplefs������ɲο� https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-store.html �������Ҫ���ܣ�ֻ��Ҫ���ļ��洢�¼��� "encrypt\_" ǰ׺���������Ĭ�ϣ��� "encrypt_default" ���ɡ���ˣ���ѡ�ļ���  "index.store.type" ֵ������

* encrypt_default 
* encrypt_fs
* encrypt_niofs
* ncrypt_mmapfs
* encrypt_simplefs

3.���ü�����Կ��index.directkey ���� index.encryptionkey

��ԿΪ16�ֽڣ�128 bit���ĳ��ȡ�

��2��������Կ�ķ�ʽ����� index.directkey �и�ֵ��������ʹ�ø�ֵ��Ϊ���ݼ��ܵ���Կ������ ���� index.encryptionkey ��Ϊ������ݼ��ܵ���Կ������  index.encryptionkey ������������Կ����Ҫͨ������Կ���ܲ��ܻ�á�

4.���û�ȡ����Կ�������ļ� encrypt.yml

	encrypt:
	    dsmhost: https://mydsm.oa?user=xxx
	    rootCA:  config_path/dsm.crt 

����˵����dsmhost�������dsmϵͳhost��rootCA�ǿ����õĸ�CA֤��·����ϵͳ�����󣬻�����dsmhost��·�����������������¸�ʽ�����ݣ������Ҫ�û�����ʵ��DSMϵͳ�ӿ�

	{
	   "dek" : "CpbCOtC9i0IuG5pBi+zl6R5iDZOSPSyxTs87zBiyLig=",
	   "ret_code" : 0,
	   "ret_msg" : ""
	}
	
���� dek��base64���ܺ��16�ֽ�128λ��Կ��


����1��ͨ��ֱ��������Կ��������Կ��ֵΪ��0123456789abcdef����base64�����Ϊ��MDEyMzQ1Njc4OWFiY2RlZg==����

	PUT twitter
	{
	    "settings" : {
	        "index" : {
		        "directkey":"MDEyMzQ1Njc4OWFiY2RlZg==",
		        "store":{
					"type":"encrypt_fs"
				}
	        }
	    }
	}

����2��ͨ��ֱ��������Կ��������Կ��ֵΪ��0123456789abcdef����������Կ������Կ���ܺ��base64ֵΪ��KpltMAJDFYrjvy/ohXmr4Q==��������Կͨ��DSMϵͳ�����ȡ������Կ��ֵΪ��aaaaaaaaaaffffff����

	PUT twitter
	{
	    "settings" : {
	        "index" : {
		        "encryptionkey":"KpltMAJDFYrjvy/ohXmr4Q==",
		        "store":{
					"type":"encrypt_fs"
				}
	        }
	    }
	}

	DSM ϵͳӦ��ʵ�ַ����������ݵĽӿڡ���YWFhYWFhYWFhYWZmZmZmZg==���ǣ�aaaaaaaaaaffffff����base64ֵ��
	{
	   "dek" : "YWFhYWFhYWFhYWZmZmZmZg==",
	   "ret_code" : 0,
	   "ret_msg" : ""
	}

for more information about

	
        
## ��ȫ˵��



index.store.type
https://www.elastic.co/guide/en/elasticsearch/reference/6.4/breaking-changes-6.0.html 
