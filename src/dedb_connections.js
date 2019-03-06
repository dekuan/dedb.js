const { EventEmitter }		= require( 'events' );
const _config			= require( 'config' );
const { DeUtilsCore }		= require( 'deutils.js' );

const DbConstants		= require( './dedb_constants' );
const DeDbMySQLConnector	= require( './connectors/dedb_mysql_connector' );





/**
 *	@class	DeDbConnections
 */
class DeDbConnections extends EventEmitter
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
		this.m_oMySQLConnector		= new DeDbMySQLConnector();
	}

	/**
	 *	Run a select query against the database.
	 *
	 *	@param	{string}	sQuery
	 *	@param  {array}		arrBindings
	 * 	@return	Promise<array>
	 */
	select( sQuery, arrBindings )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
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
	 * 	@return	Promise<array>
	 */
	insert( sQuery, arrBindings )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
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
	 * 	@return	Promise<array>
	 */
	update( sQuery, arrBindings )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
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
	 * 	@return	Promise<array>
	 */
	delete( sQuery, arrBindings )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			try
			{
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
			if ( ! this.m_oMySQLConnector )
			{
				return pfnReject( `# call Connections:query with invalid this.m_oMySQLConnector` );
			}

			try
			{
				if ( ! this.m_bEstablishedConnections )
				{
					await this._establishConnections();

					//
					//	mark as established
					//
					this.m_bEstablishedConnections	= true;
				}

				//	...
				let oConnect	= await this.m_oMySQLConnector.getConnection();
				let nTimeout	= ( DeUtilsCore.isNumeric( nTimeoutInMilliseconds ) && nTimeoutInMilliseconds > 0 ) ? nTimeoutInMilliseconds : DbConstants.MAX_QUERY_TIMEOUT;
				if ( oConnect )
				{
					oConnect.query
					({
						sql	: sQuery,
						values	: arrBindings,
						timeout	: nTimeout,
					}, ( err, arrResults, arrFields ) =>
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
						pfnR( arrResults );
					});
				}
				else
				{
					pfnReject( `# failed to getConnection` );
				}
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
 *	@type {DeDbConnections}
 */
module.exports	= DeDbConnections;