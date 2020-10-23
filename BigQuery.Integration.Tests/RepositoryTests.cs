using System;
using System.Linq;
using System.Threading.Tasks;
using BigQuery.Console.Domain;
using BigQuery.QueryBuilder;
using BigQuery.QueryBuilder.Infrastructure;
using BigQuery.QueryBuilder.Operations;
using Microsoft.VisualBasic;
using Xunit;

namespace BigQuery.Console
{
    public class RepositoryTests
    {
        [Fact]
        public async Task CreateRepositoryAndInsertData()
        {
            var repo = new UserTrackerRepository();

            await repo.EnsureTablesCreated();

            var userId = Guid.NewGuid();
            
            await repo.Users.Insert(new User
            {
                Id = userId,
                Source = "Stream-Api",
                CreatedOn = DateTime.UtcNow,
                ModifiedOn = DateTime.UtcNow,
            });

            await repo.Devices.Insert(new Device
            {
               Id = Guid.NewGuid(),
               UserId = userId,
               DispersionCoef = 0.5f,
            });

            await repo.Geos.Insert(new Geo
            {
               Id = Guid.NewGuid(),
               UserId = userId,
               Latitude = 5551.95f,
               Longitude = 13124,
               RawLocation = "пл. Верхнеторговая, 4Уфа, Башкортостан Респ., 450077"
            });

            var extractedUser = await repo.Users.Select(p => p.Id)
                .OrderByDesc(p=>p.CreatedOn)
                .First(
                Statement.And(StatementPredicate<User>.Equal(p => p.Source,"Stream-Api"),
                StatementPredicate<User>.Equal(p => p.Id, userId)));
        }
    }
}