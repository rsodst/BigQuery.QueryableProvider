using System.Threading.Tasks;
using AutoFixture;
using BigQuery.Integration.Tests.Domain;
using BigQuery.QueryBuilder.Infrastructure;
using Xunit;

namespace BigQuery.Integration.Tests
{
    public class DDLTests
    {
        private readonly Fixture _fixture;
        private readonly UsersRepository _usersRepository;

        public DDLTests()
        {
            _usersRepository = new UsersRepository(new ConnectionDetails
            {
                DatasetId = "dataset0",
                ProjectId = "bigquery-playground-290419"
            });

            _fixture = new Fixture();
        }

        [Fact]
        public async Task DropTest()
        {
            await _usersRepository.UserDetails.Drop();

            Assert.Equal(false, await _usersRepository.UserDetails.IsExists());
        }
        
        [Fact]
        public async Task CreateTest()
        {
            await _usersRepository.UserDetails.Create();
            
            Assert.Equal(true, await _usersRepository.UserDetails.IsExists());
        }
    }
}