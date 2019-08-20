const { EventEmitter }		= require( 'events' );
const { DeUtilsCore }		= require( 'deutils.js' );

const Constants			= require( './constants' );
const MySQLConnector		= require( './connectors/MySQLConnector' );







/**
 *	@class	DeDb
 */
class DeDb extends EventEmitter
{
	/**
	 *	@constructor
	 *
	 *	@param oOptions
	 */
	constructor( oOptions )
	{
		super();

		/**
		 *	all connector instances
		 */
		this.m_oOptions			= Object.assign( {}, oOptions );
		this.m_bEstablishedConnections	= false;
		this.m_oMySQLConnector		= new MySQLConnector();
	}

	/**
	 *	get the instance of MySQL Connection
	 *
	 *	@return	{Promise<{Connection}>}
	 */
	getConnection()
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				if ( ! this.m_oMySQLConnector )
				{
					return pfnReject( `# call DeDb:query with invalid this.m_oMySQLConnector` );
				}

				if ( ! this.m_bEstablishedConnections )
				{
					await this._establishConnections();

					//
					//	mark as established
					//
					this.m_bEstablishedConnections	= true;
				}

				//	...
				let oConnection = await this.m_oMySQLConnector.getConnection();
				pfnR( oConnection );
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	/**
	 *	Run a select query against the database.
	 *
	 *	@param	{string}	sQuery
	 *	@param  {array}		arrBindings
	 *	@param  {number?}	nTimeoutInMilliseconds
	 * 	@return	Promise<array>
	 */
	select( sQuery, arrBindings, nTimeoutInMilliseconds )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				let oConnection	= await this.getConnection();
				let arrResults	= await this.selectWithConnection
				(
					oConnection,
					sQuery,
					arrBindings,
					nTimeoutInMilliseconds
				);

				//	...
				pfnR( arrResults );
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	/**
	 *	Run a select query against the database.
	 *
	 *	@param	{}		oConnection
	 *	@param	{string}	sQuery
	 *	@param  {array}		arrBindings
	 *	@param  {number?}	nTimeoutInMilliseconds
	 * 	@return	Promise<array>
	 */
	selectWithConnection( oConnection, sQuery, arrBindings, nTimeoutInMilliseconds )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				//
				//	return
				//	[
				//		row object,
				//		row object,
				// 	]
				//
				let arrResults	= await this.queryWithConnection( oConnection, sQuery, arrBindings, nTimeoutInMilliseconds );
				if ( Array.isArray( arrResults ) && arrResults.length > 0 )
				{
					pfnR( arrResults );
				}
				else
				{
					pfnR( null );
				}
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	/**
	 *	Run an insert query against the database.
	 *
	 *	@param	{string}	sQuery
	 *	@param  {array}		arrBindings
	 *	@param  {number?}	nTimeoutInMilliseconds
	 * 	@return	Promise<number>	insertId
	 */
	insert( sQuery, arrBindings, nTimeoutInMilliseconds )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				let oConnection	= await this.getConnection();
				let nInsertId	= await this.insertWithConnection( oConnection, sQuery, arrBindings, nTimeoutInMilliseconds );

				//	...
				pfnR( nInsertId );
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	/**
	 *	Run an insert query against the database.
	 *
	 *	@param	{}		oConnection
	 *	@param	{string}	sQuery
	 *	@param  {array}		arrBindings
	 *	@param  {number?}	nTimeoutInMilliseconds
	 * 	@return	Promise<number>	insertId
	 */
	insertWithConnection( oConnection, sQuery, arrBindings, nTimeoutInMilliseconds )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				//
				//	return
				//	latest inserted id
				//
				let nInsertId	= null;
				let oInsertRes	= await this.queryWithConnection( oConnection, sQuery, arrBindings, nTimeoutInMilliseconds );
				if ( DeUtilsCore.isPlainObjectWithKeys( oInsertRes, [ 'insertId' ] ) &&
					DeUtilsCore.isNumeric( oInsertRes[ 'insertId' ] ) )
				{
					nInsertId = oInsertRes[ 'insertId' ];
				}

				//	...
				pfnR( nInsertId );
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	/**
	 *	Run an update query against the database.
	 *
	 *	@param	{string}	sQuery
	 *	@param  {array}		arrBindings
	 *	@param  {number?}	nTimeoutInMilliseconds
	 * 	@return	Promise<number>
	 */
	update( sQuery, arrBindings, nTimeoutInMilliseconds )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				let oConnection		= await this.getConnection();
				let nAffectedRows	= await this.updateWithConnection
				(
					oConnection,
					sQuery,
					arrBindings,
					nTimeoutInMilliseconds
				);

				//	...
				pfnR( nAffectedRows );
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	/**
	 *	Run an update query against the database.
	 *
	 *	@param	{}		oConnection
	 *	@param	{string}	sQuery
	 *	@param  {array}		arrBindings
	 *	@param  {number?}	nTimeoutInMilliseconds
	 * 	@return	Promise<number>
	 */
	updateWithConnection( oConnection, sQuery, arrBindings, nTimeoutInMilliseconds )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				//
				//	return
				//	affected rows
				//
				let nAffectedRows	= null;
				let oResult		= await this.queryWithConnection( oConnection, sQuery, arrBindings, nTimeoutInMilliseconds );
				if ( DeUtilsCore.isPlainObjectWithKeys( oResult, [ 'affectedRows' ] ) )
				{
					nAffectedRows	= oResult[ 'affectedRows' ];
				}

				//	...
				pfnR( nAffectedRows );
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	/**
	 *	Run a delete query against the database.
	 *
	 *	@param	{string}	sQuery
	 *	@param  {array}		arrBindings
	 *	@param  {number?}	nTimeoutInMilliseconds
	 * 	@return	Promise<number>	affected rows
	 */
	delete( sQuery, arrBindings, nTimeoutInMilliseconds )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				let oConnection		= await this.getConnection();
				let nAffectedRows	= await this.deleteWithConnection( oConnection, sQuery, arrBindings, nTimeoutInMilliseconds );

				//	...
				pfnR( nAffectedRows );
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	/**
	 *	Run a delete query against the database.
	 *
	 *	@param	{}		oConnection
	 *	@param	{string}	sQuery
	 *	@param  {array}		arrBindings
	 *	@param  {number?}	nTimeoutInMilliseconds
	 * 	@return	Promise<number>	affected rows
	 */
	deleteWithConnection( oConnection, sQuery, arrBindings, nTimeoutInMilliseconds )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				//
				//	return
				//	affected rows
				//
				let nAffectedRows	= null;
				let oResult		= await this.queryWithConnection( oConnection, sQuery, arrBindings, nTimeoutInMilliseconds );
				if ( DeUtilsCore.isPlainObjectWithKeys( oResult, [ 'affectedRows' ] ) )
				{
					nAffectedRows	= oResult[ 'affectedRows' ];
				}

				//	...
				pfnR( nAffectedRows );
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	/**
	 *	Execute an SQL query and return the boolean result.
	 *
	 *	@param	{string}	sQuery
	 *	@param  {array}		arrBindings
	 *	@param  {number?}	nTimeoutInMilliseconds
	 * 	@return	Promise<array>
	 */
	query( sQuery, arrBindings, nTimeoutInMilliseconds )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				let oConnection	= await this.getConnection();
				let vResults	= await this.queryWithConnection( oConnection, sQuery, arrBindings, nTimeoutInMilliseconds );

				//	...
				pfnR( vResults );
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	/**
	 *	query with a specified connection
	 *
	 *	@param	{}		oConnection
	 *	@param	{string}	sQuery
	 *	@param  {array}		arrBindings
	 *	@param  {number?}	nTimeoutInMilliseconds
	 *	@return	{Promise<array>}
	 *	@private
	 */
	queryWithConnection( oConnection, sQuery, arrBindings, nTimeoutInMilliseconds )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			if ( ! oConnection )
			{
				return pfnReject( new Error( `call queryWithConnection with invalid oConnect: ${ JSON.stringify( oConnection ) }` ) );
			}
			if ( ! DeUtilsCore.isExistingString( sQuery ) )
			{
				return pfnReject( new Error( `call queryWithConnection with invalid sQuery: ${ JSON.stringify( sQuery ) }` ) );
			}

			try
			{
				//
				//	timeout in milliseconds
				//
				let nTimeout = Constants.MAX_QUERY_TIMEOUT;
				if ( DeUtilsCore.isNumeric( nTimeoutInMilliseconds ) && nTimeoutInMilliseconds > 0 )
				{
					nTimeout = nTimeoutInMilliseconds;
				}

				oConnection.query
				({
					sql	: sQuery,
					values	: arrBindings,
					timeout	: nTimeout,

				}, ( err, vResults, arrFields ) =>
				{
					if ( err )
					{
						return pfnReject( err );
					}

					//
					//	error will be an Error if one occurred during the query
					//	results will contain the results of the query
					//	fields will contain information about the returned results fields (if any)
					//
					pfnR( vResults );
				});
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	transaction()
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				let oConnection = await this.getConnection();
				if ( ! oConnection )
				{
					return pfnReject( `# failed to getConnection` );
				}

				//	...
				let pfnQuery = ( sQuery, arrBindings, nTimeoutInMilliseconds ) =>
				{
					return new Promise( async ( pfnQueryR, pfnQueryReject ) =>
					{
						try
						{
							let vResult = await this.queryWithConnection
							(
								oConnection,
								sQuery,
								arrBindings,
								nTimeoutInMilliseconds
							);
							pfnQueryR( vResult );
						}
						catch ( vException )
						{
							oConnection.rollback( () =>
							{
								pfnQueryReject( vException );
							});
						}
					});
				};
				let pfnCommit = () =>
				{
					return new Promise( async ( pfnR, pfnReject ) =>
					{
						try
						{
							oConnection.commit( err =>
							{
								if ( err )
								{
									return oConnection.rollback( () =>
									{
										throw err;
									});
								}
							});
						}
						catch ( vException )
						{
							pfnReject( vException );
						}
					});
				};

				//	...
				pfnR
				({
					query	: pfnQuery,
					commit	: pfnCommit,
				});
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}


	/**
	 *	establish connections
	 *
	 *	@return {Promise<{}>}
	 *	@private
	 */
	_establishConnections()
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				let nMySQLConnectedCount = await this._establishMySQLConnection();

				//	...
				pfnR
				({
					mysql	: nMySQLConnectedCount
				});
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	/**
	 *	establish MySQL connection
	 *
	 *	@return {Promise<number>}
	 *	@private
	 */
	_establishMySQLConnection()
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
				//
				//	try to build a connection to MySQL server
				//
				if ( ! DeUtilsCore.isPlainObject( this.m_oOptions ) ||
					0 === Object.keys( this.m_oOptions ).length )
				{
					return pfnReject( `# database connection configuration is invalid.` );
				}

				//	...
				let nConnectedCount	= await this.m_oMySQLConnector.connect( this.m_oOptions );
				pfnR( nConnectedCount );
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}
}





/**
 *	@exports
 *	@type {DeDb}
 */
module.exports	= DeDb;