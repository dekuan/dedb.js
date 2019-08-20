const assert			= require( 'assert' );
const _config			= require( 'config' );
const { DeDb }			= require( '../index' );
const { DeUtilsCore }		= require( 'deutils.js' );


//
//	mocha --timeout 10000
//
const TEST_DATABASE_NAME	= `dedb`;
const TEST_TABLE_NAME		= `test_connections`;
const DB_CONFIGURATION		= _config.get( 'connections.mysql' ) || {};





describe( 'DeDb.test', () =>
{
	it( 'should drop table named test_connections if it exists', async () =>
	{
		const oConnections	= new DeDb( DB_CONFIGURATION );

		try
		{
			let oOkPacketConnections = await oConnections.query
			(
				'DROP TABLE IF EXISTS test_connections',
				[]
			);
			let oOkPacketTransaction = await oConnections.query
			(
				'DROP TABLE IF EXISTS test_transaction',
				[]
			);

			assert.ok( DeUtilsCore.isPlainObjectWithKeys( oOkPacketConnections, 'protocol41' ) && true === oOkPacketConnections[ 'protocol41' ] );
			assert.ok( DeUtilsCore.isPlainObjectWithKeys( oOkPacketTransaction, 'protocol41' ) && true === oOkPacketTransaction[ 'protocol41' ] );
		}
		catch ( vException )
		{
			console.error( vException );
		}
	});

	it( 'should create a table named test_connections', async () =>
	{
		const oConnections	= new DeDb( DB_CONFIGURATION );
		const sCreateTable	= 'CREATE TABLE `test_connections` (\n' +
			'  `tc_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n' +
			'  `tc_mid` binary(16) DEFAULT NULL,\n' +
			'  `tc_cdate` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,\n' +
			'  `tc_mdate` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n' +
			'  PRIMARY KEY (`tc_id`)\n' +
			') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci';

		try
		{
			let oOkPacket = await oConnections.query( sCreateTable, [] );
			assert.ok( DeUtilsCore.isPlainObjectWithKeys( oOkPacket, 'protocol41' ) && true === oOkPacket[ 'protocol41' ] );
		}
		catch ( vException )
		{
			console.error( vException );
		}
	});

	it( 'should create a table named test_transaction', async () =>
	{
		const oConnections	= new DeDb( DB_CONFIGURATION );
		const sCreateTable	= 'CREATE TABLE `test_transaction` (\n' +
			'  `tt_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n' +
			'  `tt_mid` binary(16) DEFAULT NULL,\n' +
			'  `tc_id` bigint(20) unsigned NOT NULL,\n' +
			'  `tt_cdate` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,\n' +
			'  `tt_mdate` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n' +
			'  PRIMARY KEY (`tt_id`)\n' +
			') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci';

		try
		{
			let oOkPacket = await oConnections.query( sCreateTable, [] );
			assert.ok( DeUtilsCore.isPlainObjectWithKeys( oOkPacket, 'protocol41' ) && true === oOkPacket[ 'protocol41' ] );
		}
		catch ( vException )
		{
			console.error( vException );
		}
	});

	it( 'table named test_connections should exists', async () =>
	{
		const oConnections	= new DeDb( DB_CONFIGURATION );

		try
		{
			let arrResultSelect = await oConnections.select
			(
				"SELECT * FROM information_schema.tables WHERE table_schema = ? AND table_name = ? LIMIT 1;",
				[ TEST_DATABASE_NAME, TEST_TABLE_NAME ]
			);
			let bExists	= Array.isArray( arrResultSelect ) && arrResultSelect.length > 0 &&
				DeUtilsCore.isPlainObjectWithKeys( arrResultSelect[ 0 ], [ 'TABLE_NAME' ] ) &&
				'test_connections' === arrResultSelect[ 0 ][ 'TABLE_NAME' ];
			assert.ok( bExists );
		}
		catch ( vException )
		{
			console.error( vException );
		}
	});


	it( 'should insert a record into table test_connections', async () =>
	{
		try
		{
			const oConnections	= new DeDb( DB_CONFIGURATION );
			let nInsertId		= await oConnections.insert
			(
				"INSERT INTO test_connections( tc_mid ) VALUES( UUID_TO_BIN( UUID() ) )",
				[]
			);
			assert.ok( nInsertId > 0 );
		}
		catch ( vException )
		{
			console.error( vException );
		}
	});


	it( 'should query the record from table test_connections inserted at previous step', async () =>
	{
		try
		{
			const oConnections	= new DeDb( DB_CONFIGURATION );
			let arrResults		= await oConnections.select
			(
				"SELECT tc_id, BIN_TO_UUID( tc_mid ) AS t_tc_mid, tc_cdate, tc_mdate FROM test_connections LIMIT 1",
				[]
			);

			assert.ok
			(
				Array.isArray( arrResults ) && arrResults.length > 0 &&
				DeUtilsCore.isPlainObjectWithKeys( arrResults[ 0 ], [ 'tc_id', 't_tc_mid', 'tc_cdate', 'tc_mdate' ] ) &&
				DeUtilsCore.isNumeric( arrResults[ 0 ][ 'tc_id' ] ) &&
				arrResults[ 0 ][ 'tc_id' ] > 0 &&
				DeUtilsCore.isExistingString( arrResults[ 0 ][ 't_tc_mid' ] ) &&
				'[object Date]' === Object.prototype.toString.call( arrResults[ 0 ][ 'tc_cdate' ] ) &&
				'[object Date]' === Object.prototype.toString.call( arrResults[ 0 ][ 'tc_mdate' ] )
			);
		}
		catch ( vException )
		{
			console.error( vException );
		}
	});


	it( 'should perform a transactions on table test_connections and test_transaction', async () =>
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				const oDeDb	= new DeDb( DB_CONFIGURATION );
				const oConnect	= await oDeDb.getConnection();
				oConnect.beginTransaction( ( err ) =>
				{
					if ( err )
					{
						throw err;
					}

					//	...
					oConnect.query( 'INSERT INTO test_connections( tc_mid ) VALUES( UUID_TO_BIN( UUID() ) )', ( sError, oResults, arrFields ) =>
					{
						if ( sError )
						{
							return oConnect.rollback( () =>
							{
								throw sError;
							});
						}

						let nNewTcId = oResults[ 'insertId' ];
						oConnect.query( 'INSERT INTO test_transaction( tt_mid, tc_id ) VALUES( UUID_TO_BIN( UUID() ), ? )', nNewTcId, ( sError, arrResults, arrFields ) =>
						{
							if ( sError )
							{
								return oConnect.rollback( () =>
								{
									throw sError;
								});
							}

							//	...
							oConnect.commit( ( err ) =>
							{
								if ( err )
								{
									return oConnect.rollback( () =>
									{
										throw err;
									});
								}

								//	...
								assert.ok( true );

								//	...
								pfnR( null );
							});
						});
					});
				});
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	});
});
