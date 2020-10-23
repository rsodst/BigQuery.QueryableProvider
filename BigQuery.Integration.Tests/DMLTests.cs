using System;
using System.Linq;
using System.Threading.Tasks;
using AutoFixture;
using BigQuery.Integration.Tests.Domain;
using BigQuery.QueryBuilder;
using BigQuery.QueryBuilder.Infrastructure;
using BigQuery.QueryBuilder.Operations;
using FluentAssertions;
using Xunit;

namespace BigQuery.Integration.Tests
{
    public class DMLTests
    {
        private readonly Fixture _fixture;
        private readonly UsersRepository _usersRepository;

        public DMLTests()
        {
            _usersRepository = new UsersRepository(new ConnectionDetails
            {
                DatasetId = "dataset0",
                ProjectId = "bigquery-playground-290419"
            });

            _fixture = new Fixture();

            _usersRepository.UserDetails.Drop().ConfigureAwait(false).GetAwaiter().GetResult();
            _usersRepository.UserDetails.Create().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        [Fact]
        public async Task InsertSelectMultipleRowTest_CompleteInsertSelect()
        {
            var data = _fixture.Create<UserDetail>();

            var c= await _usersRepository.UserDetails.Insert(data);

            var selectedData = await _usersRepository.UserDetails.Select()
                .First(StatementPredicate<UserDetail>.Equal(p => p.Id, data.Id));

            data.Should().BeEquivalentTo(selectedData);
        }

        [Fact]
        public async Task InsertSelectWithSelectorsTest_SelectParticularyEntityProperties()
        {
            var data = _fixture.Create<UserDetail>();

            var expectedData = _fixture
                .Build<UserDetail>()
                .With(p => p.Id, data.Id)
                .Without(p => p.Data)
                .Create();

            await _usersRepository.UserDetails.Insert(data);

            var selectedData = await _usersRepository.UserDetails.Select(p => p.Id)
                .First(StatementPredicate<UserDetail>.Equal(p => p.Id, data.Id));

            expectedData.Should().BeEquivalentTo(selectedData);
        }

        [Fact]
        public async Task InsertSelectMultipleRowTest_CompleteInsertSelectMultipleRows()
        {
            var key = _fixture.Create<string>();

            var details = new[]
            {
                _fixture.Build<UserDetail>().With(p => p.Data, key).Create(),
                _fixture.Build<UserDetail>().With(p => p.Data, key).Create(),
                _fixture.Build<UserDetail>().With(p => p.Data, key).Create()
            };

            await _usersRepository.UserDetails.Insert(details);

            var selectedData = await _usersRepository.UserDetails.Select()
                .ToList(StatementPredicate<UserDetail>.Equal(p => p.Data, key));

            details.Should().BeEquivalentTo(selectedData);
        }

        [Fact]
        public async Task InsertSelectMultipleRowWithOrderByTest_CompleteInsertSelectOrderedMultipleRows()
        {
            if (!await _usersRepository.UserDetails.IsExists())
            {
                throw new Exception("Table not exists");
            }
            
            var details = new[]
            {
                _fixture.Build<UserDetail>().With(p => p.Data, "1").Create(),
                _fixture.Build<UserDetail>().With(p => p.Data, "2").Create(),
                _fixture.Build<UserDetail>().With(p => p.Data, "3").Create()
            };

            var t = await _usersRepository.UserDetails.Insert(details);

            var selectedData = await _usersRepository.UserDetails
                .Select()
                .OrderBy(p => p.Data)
                .ToList(
                    Statement.Or(Statement.Or(
                            StatementPredicate<UserDetail>.Equal(p => p.Data, "1"),
                            StatementPredicate<UserDetail>.Equal(p => p.Data, "2")),
                        StatementPredicate<UserDetail>.Equal(p => p.Data, "3")));

            details.Should().BeEquivalentTo(selectedData);
        }

        [Fact]
        public async Task InsertSelectMultipleRowWithOrderByDescTest_CompleteInsertSelectOrderedDescMultipleRows()
        {
            var details = new[]
            {
                _fixture.Build<UserDetail>().With(p => p.Data, "4").Create(),
                _fixture.Build<UserDetail>().With(p => p.Data, "5").Create(),
                _fixture.Build<UserDetail>().With(p => p.Data, "6").Create()
            };

            await _usersRepository.UserDetails.Insert(details);

            var selectedData = await _usersRepository.UserDetails
                .Select()
                .OrderByDesc(p => p.Data)
                .ToList(
                    Statement.Or(Statement.Or(
                            StatementPredicate<UserDetail>.Equal(p => p.Data, "4"),
                            StatementPredicate<UserDetail>.Equal(p => p.Data, "5")),
                        StatementPredicate<UserDetail>.Equal(p => p.Data, "6")));

            details.OrderByDescending(p => p.Data).Should().BeEquivalentTo(selectedData);
        }
    }
}