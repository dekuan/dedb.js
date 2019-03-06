const { EventEmitter }			= require( 'events' );
const _mysql				= require( 'mysql' );
const DbConstants			= require( '../dedb_constants' );
const { DeUtilsCore }			= require( 'deutils.js' );






/**
 *	@class	DeDbMySQLConnector
 */
class DeDbMySQLConnector extends EventEmitter
{
	/**
	 *	@constructor
	 */
	constructor()
	{
		super();

		//
		//	configuration options
		//
		this.m_oOptions		= null;
		this.m_oPoolCluster	= null;
	}

	/**
	 *	connect to the MySQL server
	 *
	 *	@param	{}	oOptions
	 *	@return	{Promise<number>}
	 */
	connect( oOptions )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			if ( ! DeUtilsCore.isPlainObject( oOptions ) ||
				0 === Object.keys( oOptions ).length )
			{
				return pfnReject( `# call MySQLConnector::connect with invalid oOptions` );
			}

			try
			{
				//	...
				let nSuccessCount	= 0;

				//
				//	create a pool cluster
				//
				this.m_oPoolCluster	= _mysql.createPoolCluster();

				let arrNodeNames	= Object.keys( oOptions );
				for ( let i = 0; i < arrNodeNames.length; i ++ )
				{
					let sNodeName	= arrNodeNames[ i ];
					let oConfig	= Object.assign
					(
						{},
						{ connectionLimit : DbConstants.MAX_POOL_CONNECTION_LIMIT },
						oOptions[ sNodeName ]
					);
					if ( this.isValidMySQLConfig( oConfig ) )
					{
						//	add a named configuration
						this.m_oPoolCluster.add( sNodeName, oConfig );

						//	...
						nSuccessCount ++;
					}
				}

				//	...
				pfnR( nSuccessCount );
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	/**
	 *	get connection
	 *
	 *	@param	{string?}	sGroup
	 *	@return {Promise<{}>}	oConnection
	 */
	getConnection( sGroup )
	{
		return new Promise( async ( pfnR, pfnReject ) =>
		{
			if ( ! this.m_oPoolCluster )
			{
				return pfnReject( `# getConnection with initialized m_oPoolCluster: ${ JSON.stringify( this.m_oPoolCluster ) }` );
			}

			try
			{
				if ( DeUtilsCore.isExistingString( sGroup ) )
				{
					this.m_oPoolCluster.getConnection( sGroup, ( err, oConnection ) =>
					{
						if ( err )
						{
							return pfnReject( err );
						}

						//	...
						pfnR( oConnection );
					});
				}
				else
				{
					//	Target Group : ALL( anonymous, MASTER, SLAVE1-2 ), Selector : round-robin(default)
					this.m_oPoolCluster.getConnection( ( err, oConnection ) =>
					{
						if ( err )
						{
							return pfnReject( err );
						}

						//	...
						pfnR( oConnection );
					});
				}
			}
			catch ( vException )
			{
				pfnReject( vException );
			}
		});
	}

	/**
	 *	check if the oDbConfig is a valid MySQL configuration
	 *
	 *	@param	{}	oOptions
	 *	@return	{boolean}
	 */
	isValidMySQLConfig( oOptions )
	{
		return DeUtilsCore.isPlainObject( oOptions ) &&
			DeUtilsCore.isExistingString( oOptions.host ) &&
			DeUtilsCore.isExistingString( oOptions.user ) &&
			DeUtilsCore.isExistingString( oOptions.password ) &&
			DeUtilsCore.isExistingString( oOptions.database )
		;
	}

}





/**
 *	@exports
 *	@type {DeDbMySQLConnector}
 */
module.exports	= DeDbMySQLConnector;
